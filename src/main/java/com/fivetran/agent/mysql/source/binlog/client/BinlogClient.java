/**
 * Copyright (c) Fivetran 2018
 **/
package com.fivetran.agent.mysql.source.binlog.client;

import com.fivetran.agent.mysql.Main;
import com.fivetran.agent.mysql.ReadSourceLog;
import com.fivetran.agent.mysql.config.DatabaseCredentials;
import com.fivetran.agent.mysql.log.BinlogDisconnectedException;
import com.fivetran.agent.mysql.log.BinlogTimeout;
import com.fivetran.agent.mysql.log.NoBinlogEventsReceived;
import com.fivetran.agent.mysql.source.BinlogPosition;
import com.fivetran.agent.mysql.source.SourceEvent;
import com.fivetran.agent.mysql.source.SourceEventType;
import com.fivetran.agent.mysql.source.binlog.BinlogEvent;
import com.fivetran.agent.mysql.source.binlog.BinlogInputStream;
import com.fivetran.agent.mysql.source.binlog.client.shyiko.Greeting;
import com.fivetran.agent.mysql.source.binlog.client.shyiko.BinlogChannel;
import com.fivetran.agent.mysql.source.binlog.parser.*;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.AsynchronousSocketChannel;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class BinlogClient implements ReadSourceLog {

    private static final int END_OF_STREAM = -1;
    private static final int CONNECT_TIMEOUT = 60 * 1000;
    private static final int BINLOG_STARTING_POSITION = 4;
    private static final int PACKET_HEADER_LENGTH = 5;

    public static final int ONE_BYTE = 1;
    public static final int TWO_BYTES = 2;
    public static final int THREE_BYTES = 3;
    public static final int FOUR_BYTES = 4;
    public static final int FIVE_BYTES = 5;
    public static final int SIX_BYTES = 6;
    public static final int EIGHT_BYTES = 8;

    private final DatabaseCredentials creds;
    protected final Map<Long, TableMapEventBody> tableMaps = new HashMap<>();

    enum ChecksumType {
        NONE(0),
        CRC32(4);

        private int length;

        ChecksumType(int length) {
            this.length = length;
        }

        public int getLength() {
            return length;
        }
    }

    public BinlogClient(DatabaseCredentials creds) {
        this.creds = creds;
    }

    public PacketChannel connect() {
        try {
            AsynchronousSocketChannel socketChannel = AsynchronousSocketChannel.open();
            Future<Void> future = socketChannel.connect(new InetSocketAddress(creds.host, creds.port));
            future.get(CONNECT_TIMEOUT, TimeUnit.MILLISECONDS);

            PacketChannel channel = new BinlogChannel(socketChannel);

            Greeting greeting = channel.getGreeting();
            channel.writeGreetingResponse(creds.user, creds.password, greeting.serverCollation, greeting.scramble);

            return channel;
        } catch (IOException | InterruptedException | TimeoutException | ExecutionException e) {
            throw new RuntimeException("Connection to MySQL server failed", e);
        }
    }

    @Override
    public BinlogPosition currentPosition() {
        try (PacketChannel channel = connect()) {
            List<String[]> resultSet = channel.queryForResultSet("show master status");
            String[] resultSetValues = resultSet.get(0);

            String binlogFilename = resultSetValues[0];
            long binlogPosition = Long.parseLong(resultSetValues[1]);

            if (binlogPosition < BINLOG_STARTING_POSITION) {
                binlogPosition = BINLOG_STARTING_POSITION;
            }

            return new BinlogPosition(binlogFilename, binlogPosition);
        } catch (IOException | TimeoutException e) {
            throw new RuntimeException("Connection failed", e);
        }
    }

    @Override
    public EventReader events(BinlogPosition startPosition) {
        return new EventReader() {
            PacketChannel channel = connect();
            Iterator<BinlogEvent> rawEvents = binlogStreamIterator(channel, startPosition);
            BinlogPosition latestPosition = startPosition;

            @Override
            public SourceEvent readEvent() {
                try {
                    BinlogEvent event = rawEvents.next();

                    latestPosition = event.getCurrentPosition();

                    return getSourceEvent(event);
                } catch (BinlogDisconnectedException | NoBinlogEventsReceived e) {
                    reconnect();
                    return SourceEvent.createTimeout(latestPosition);
                }
            }

            private void reconnect() {
                try {
                    if (channel.isOpen()) {
                        channel.close();
                    }
                    channel = connect();
                    rawEvents = binlogStreamIterator(channel, latestPosition);
                } catch (IOException io) {
                    throw new RuntimeException(io);
                }
            }

            @Override
            public void close() throws Exception {
                if (channel.isOpen())
                    channel.close();
            }
        };
    }

    private Iterator<BinlogEvent> binlogStreamIterator(PacketChannel channel, BinlogPosition startPosition) {
        try {
            int checksumLength = getBinlogChecksumLength(channel);
            requestBinaryLogStream(startPosition, channel);

            return new Iterator<BinlogEvent>() {
                BinlogPosition latestPosition = startPosition;

                @Override
                public boolean hasNext() {
                    return true;
                }

                @Override
                public BinlogEvent next() {
                    try {
                        checkPacketLength(channel.read(PACKET_HEADER_LENGTH));

                        EventHeader eventHeader = readHeader(channel.read(EventHeader.HEADER_LENGTH));

                        EventBody eventBody =
                                readBody(
                                        channel.read(eventHeader.getBodyLength()),
                                        eventHeader,
                                        tableMaps,
                                        checksumLength);
                        BinlogPosition currentPosition =
                                updatedBinlogPosition(eventHeader, eventBody, latestPosition);

                        latestPosition = currentPosition;

                        return new BinlogEvent(eventHeader, eventBody, currentPosition);
                    } catch (TimeoutException e) {
                        Main.LOG.log(new BinlogTimeout());
                        throw new NoBinlogEventsReceived(e);
                    } catch (IOException e) {
                        throw new BinlogDisconnectedException(e);
                    }
                }
            };
        } catch (TimeoutException | IOException e) {
            throw new RuntimeException("Connection failed");
        }
    }

    private int getBinlogChecksumLength(PacketChannel channel) throws IOException, TimeoutException {
        List<String[]> resultSet = channel.queryForResultSet("show global variables like 'binlog_checksum'");

        if (resultSet.size() == 0) {
            return 0;
        }
        confirmSupportOfChecksum(channel);

        String[] resultSetValues = resultSet.get(0);
        ChecksumType checksumType = ChecksumType.valueOf(resultSetValues[1].toUpperCase());

        return checksumType.getLength();
    }

    private void confirmSupportOfChecksum(PacketChannel channel) throws IOException, TimeoutException {
        try {
            channel.queryToSetVariable("set @master_binlog_checksum = @@global.binlog_checksum");
        } catch (PacketReadException e) {
            throw new RuntimeException("Failure to confirm support of checksum");
        }
    }

    private void requestBinaryLogStream(BinlogPosition binlogPosition, PacketChannel channel)
            throws IOException, TimeoutException {
        List<String[]> resultSet = channel.queryForResultSet("select @@server_id");
        if (resultSet.size() != 1) {
            throw new RuntimeException("Unable to read server_id");
        }
        long serverId = Long.valueOf(resultSet.get(0)[0]);

        channel.initializeBinlogDump(serverId, binlogPosition);
    }

    private EventHeader readHeader(byte[] headerBytes) {
        EventHeaderParser headerParser = new EventHeaderParser();
        return headerParser.parse(headerBytes);
    }

    private EventBody readBody(byte[] bodyBytes,
                               EventHeader eventHeader,
                               Map<Long, TableMapEventBody> tableMaps,
                               int checksumLength)
            throws IOException {
        EventBodyParser bodyParser = new EventBodyParser();

        byte[] minusChecksum = Arrays.copyOfRange(bodyBytes, 0, bodyBytes.length - checksumLength);
        return bodyParser.parse(minusChecksum, eventHeader.getType(), tableMaps);
    }

    private void checkPacketLength(byte[] bytes) {
        BinlogInputStream in = new BinlogInputStream(bytes);
        int packetLength = in.readHeaderPacketLength();

        if (packetLength == END_OF_STREAM)
            throw new RuntimeException("Error reading packet header");
    }

    private BinlogPosition updatedBinlogPosition(EventHeader header, EventBody body, BinlogPosition latestPosition) {
        if (header.getType() == EventType.ROTATE) {
            return new BinlogPosition(
                    ((RotateEventBody) body).getBinlogFilename(),
                    ((RotateEventBody) body).getBinlogPosition());
        }
        if (header.getType() == EventType.FORMAT_DESCRIPTION) {
            return latestPosition;
        }
        return new BinlogPosition(latestPosition.file, header.getNextPosition());
    }

    private SourceEvent getSourceEvent(BinlogEvent event) {
        EventHeader header = event.getHeader();
        EventBody body = event.getBody();
        BinlogPosition binlogPosition = event.getCurrentPosition();
        switch (header.getType()) {
            case EXT_WRITE_ROWS:
                return SourceEvent.createInsert(
                        ((ModifyingEventBody) body).getTableRef(),
                        new BinlogPosition(binlogPosition.file, header.getNextPosition()),
                        ((ModifyingEventBody) body).getNewRows());
            case EXT_UPDATE_ROWS:
                return SourceEvent.createUpdate(
                        ((ModifyingEventBody) body).getTableRef(),
                        new BinlogPosition(binlogPosition.file, header.getNextPosition()),
                        ((ModifyingEventBody) body).getOldRows(),
                        ((ModifyingEventBody) body).getNewRows());
            case EXT_DELETE_ROWS:
                return SourceEvent.createDelete(
                        ((ModifyingEventBody) body).getTableRef(),
                        new BinlogPosition(binlogPosition.file, header.getNextPosition()),
                        ((ModifyingEventBody) body).getNewRows());
            default:
                return SourceEvent.createOther(binlogPosition);
        }
    }
}
