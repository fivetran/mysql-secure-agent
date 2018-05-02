/**
* Copyright (c) Fivetran 2018
**/
package com.fivetran.agent.mysql.binlog_test_generator;

import com.fivetran.agent.mysql.config.DatabaseCredentials;
import com.fivetran.agent.mysql.source.BinlogPosition;
import com.fivetran.agent.mysql.source.QueryDatabase;
import com.fivetran.agent.mysql.source.binlog.parser.EventType;

import javax.sql.DataSource;
import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

public class CaptureBinlogEvents {

    // todo: likely move this along with related classes to another repo once tests no longer need to be generated

    private static final int MAX_ATTEMPTS = 10;

    private Socket socket;
    private Connection conn;
    private InputStream in;
    private DatabaseCredentials creds;
    private String schema = "capture_binlog_events";
    private String table = schema + ".foo";
    private Integer peek = null;
    private int checksumLength = 0;
    private static LinkedList<BinlogTest> tests = BinlogTest.generateTests();
    private TestEvent tableMap;

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

    // https://dev.mysql.com/doc/dev/mysql-server/latest/my__command_8h_source.html
    public enum CommandType {
        SLEEP,
        QUIT,
        INIT_DB,
        QUERY,
        FIELD_LIST,
        CREATE_DB,
        DROP_DB,
        REFRESH,
        DEPRECATED_1, /* deprecated, used to be COM_SHUTDOWN */
        STATISTICS,
        PROCESS_INFO,
        CONNECT,
        PROCESS_KILL,
        DEBUG,
        PING,
        TIME,
        DELAYED_INSERT,
        CHANGE_USER,
        BINLOG_DUMP,
        TABLE_DUMP,
        CONNECT_OUT,
        REGISTER_SLAVE,
        STMT_PREPARE,
        STMT_EXECUTE,
        STMT_SEND_LONG_DATA,
        STMT_CLOSE,
        STMT_RESET,
        SET_OPTION,
        STMT_FETCH,
        DAEMON,
        BINLOG_DUMP_GTID,
        RESET_CONNECTION
    }

    // choose the tests you want run in BinlogTest#generateTests()
    // to feed the test output directly into the BinlogClient, set PARSER_TESTING to true
    public static void main(String[] args) throws SQLException, IOException, InterruptedException {
        System.out.print("class BinlogParserSpec {\n");

        for (int i = 0; i < MAX_ATTEMPTS; ++i) {
            try {
                setup();
                break;
            } catch (RuntimeException e) {
                Thread.sleep(500);
            }
        }

        System.out.print("}\n");
    }

    private static void setup() throws SQLException, IOException {
        CaptureBinlogEvents client = new CaptureBinlogEvents("localhost", "", "", 3306);
        client.connect();
        client.captureEvents();
        client.close();
    }

    private CaptureBinlogEvents(String host, String user, String password, int port) {
        creds = new DatabaseCredentials(host, port, user, password);
    }

    private void connect() throws SQLException {
        DataSource DB = QueryDatabase.getDataSource(creds);

        conn = DB.getConnection();
        socket = new Socket();
    }

    public void close() throws SQLException, IOException {
        conn.close();
        in.close();
        socket.close();
    }

    private void captureEvents() throws IOException {
        setupDatabase();
        in = setupBinlogCapture(socket);
        captureEventTests(in);
    }


    private void captureEventTests(InputStream in) throws IOException {
        while (tests.size() > 0) {
            BinlogTest test = tests.peekFirst();

            String query = "CREATE TABLE " + table +
                    " (" + test.getColumn() + " " + test.getType()
                    + " " + ((test.getCharSet().isPresent()) ? test.getCharSet().get() : "") + ")";
            execute(conn, query);

            query = "INSERT INTO " + table + " (" + test.getColumn() + ") VALUES (" + test.getValue() + ")";
            runQuery(query, in, test);

            switch (test.getEventType()) {
                case UPDATE:
                    query = "UPDATE " + table + " SET " + test.getColumn() + " = " + test.getUpdateValue().get();
                    runQuery(query, in, test);
                    break;
                case DELETE:
                    query = "DELETE FROM " + table;
                    runQuery(query, in, test);
                    break;
            }

            query = "DROP TABLE " + table;
            execute(conn, query);

            tests.pop();
        }
    }

    private void runQuery(String query, InputStream in, BinlogTest event) throws IOException {
        execute(conn, query);
        capture(query, in, event);
    }

    private void setupDatabase() {
        execute(
                conn,
                "DROP DATABASE IF EXISTS " + schema,
                "CREATE DATABASE " + schema,
                "USE " + schema);
    }

    private InputStream setupBinlogCapture(Socket socket) throws IOException {
        socket.connect(new InetSocketAddress(creds.host, creds.port));
        InputStream in = new BufferedInputStream(socket.getInputStream());
        OutputStream out = socket.getOutputStream();

        Greeting greeting = receiveGreeting(in);
        authenticate(in, out, creds.user, creds.password, greeting.serverCollation, greeting.scramble);

        BinlogPosition binlogPosition = fetchBinlogFilenameAndPosition(in, out);
        checksumLength = getBinlogChecksumLength(in, out);
        requestBinaryLogStream(out, 65535, binlogPosition);

        socket.setSoTimeout(2 * 1000);

        return in;
    }

    private int getBinlogChecksumLength(InputStream in, OutputStream out) throws IOException {
        byte[] query = QueryCommand.create("show global variables like 'binlog_checksum'");
        writePacket(out, query, 0);

        List<String[]> resultSet = readResultSet(in);
        if (resultSet.size() == 0) {
            return 0;
        }

        confirmSupportOfChecksum(in, out);

        String[] resultSetValues = resultSet.get(0);
        ChecksumType checksumType = ChecksumType.valueOf(resultSetValues[1].toUpperCase());

        return checksumType.getLength();
    }

    private void confirmSupportOfChecksum(InputStream in, OutputStream out) throws IOException {
        byte[] query = QueryCommand.create("set @master_binlog_checksum= @@global.binlog_checksum");
        writePacket(out, query, 0);

        if (!isOK(readPacket(in))) {
            throw new RuntimeException("Failure to confirm support of checksum");
        }
    }

    private boolean isEOF(byte[] packet) {
        return packet[0] == (byte) 0xFE;
    }

    private boolean isOK(byte[] packet) {
        return packet[0] == (byte) 0x00;
    }

    private void requestBinaryLogStream(
            OutputStream out, long serverId, BinlogPosition binlogPosition)
            throws IOException {

        byte[] command =
                DumpBinaryLogCommand.create(serverId, binlogPosition.file, binlogPosition.position);
        writePacket(out, command, 0);
    }

    private void authenticate(
            InputStream in,
            OutputStream out,
            String user,
            String password,
            int collation,
            String salt)
            throws IOException {
        byte[] authPacket = AuthenticateCommand.greetingResponse(user, password, collation, salt);
        writePacket(out, authPacket, 1);

        if (!isOK(readPacket(in))) {
            throw new RuntimeException("Authentication failure");
        }
    }

    private Greeting receiveGreeting(InputStream in) throws IOException {
        byte[] packet = readPacket(in);
        return new Greeting(packet);
    }

    private List<String[]> readResultSet(InputStream in) throws IOException {
        readPacket(in);

        while (!isEOF(readPacket(in))) {
            /* skip until eof_packet */
        }

        List<String[]> resultSet = new ArrayList<>();
        for (byte[] bytes; !isEOF(bytes = readPacket(in)); ) {
            resultSet.add(readResultSetRow(bytes));
        }

        return resultSet;
    }

    private BinlogPosition fetchBinlogFilenameAndPosition(InputStream in, OutputStream out)
            throws IOException {
        byte[] query = QueryCommand.create("show master status");
        writePacket(out, query, 0);

        List<String[]> resultSet = readResultSet(in);

        String[] resultSetValues = resultSet.get(0);
        String binlogFilename = resultSetValues[0];
        long binlogPosition = Long.parseLong(resultSetValues[1]);

        if (binlogPosition < 4) {
            binlogPosition = 4;
        }

        return new BinlogPosition(binlogFilename, binlogPosition);
    }

    private String[] readResultSetRow(byte[] bytes) throws IOException {
        ByteArrayInputStream buffer = new ByteArrayInputStream(bytes);
        List<String> values = new ArrayList<>();
        while (buffer.available() > 0) {
            values.add(PacketUtil.readLengthEncodedString(buffer));
        }
        return values.toArray(new String[values.size()]);
    }

    private void writePacket(OutputStream out, byte[] packet, int packetNumber) throws IOException {
        PacketUtil.writeInt(out, packet.length, 3); // packet length
        PacketUtil.writeInt(out, packetNumber, 1);
        out.write(packet, 0, packet.length);
        out.flush();
    }

    private byte[] readPacket(InputStream in) throws IOException {
        if (peek(in) == (byte) 0xFF /* error */) {
            throw new RuntimeException("Server Exception");
        }

        int payloadLength = readPayloadLength(in);
        in.skip(1); // Sequence ID
        return readPayload(in, payloadLength);
    }

    private int readPacketHeader(InputStream in) throws IOException {
        int packetLength = readPayloadLength(in);
        in.skip(1); // Sequence ID
        int marker = read(in);

        if (marker == 0xFF)
            return -1;

        return packetLength;
    }

    private void capture(String query, InputStream in, BinlogTest testInfo) throws IOException {
        String column = testInfo.getColumn();
        try {
            while (peek(in) != -1 /* end of stream */) {
                TestEvent event = readEvent(in, column, query);

                event.setOldValue(testInfo.getValue());
                if (testInfo.getUpdateValue().isPresent())
                    event.setNewValue(testInfo.getUpdateValue().get());

                if (event.getType() == EventType.TABLE_MAP)
                    tableMap = event;

                event.setTableMap(tableMap);

                switch (testInfo.getEventType()) {
                    case FULL_EVENT:
                        if (event.getType() == EventType.EXT_WRITE_ROWS) {
                            event.printFullEventHex();
                        }
                        break;
                    case HEADER:
                        if (event.getType() == EventType.EXT_WRITE_ROWS)
                            event.printHeader();
                        break;
                    case WRITE:
                        if (event.getType() == EventType.EXT_WRITE_ROWS)
                            event.printWriteBody(testInfo.getValue());
                        break;
                    case DELETE:
                        if (event.getType() == EventType.EXT_DELETE_ROWS)
                            event.printWriteBody(testInfo.getValue());
                        break;
                    case UPDATE:
                        if (event.getType() == EventType.EXT_UPDATE_ROWS)
                            event.printWriteBody(testInfo.getUpdateValue().get());
                        break;
                    case TABLEMAP:
                        if (event.getType() == EventType.EXT_WRITE_ROWS)
                            event.printTableMap();
                        break;
                    case MASSIVE:
                        if (event.getType() == EventType.EXT_WRITE_ROWS)
                            event.printMassive();
                        break;
                }
            }
        } catch (SocketTimeoutException ignored) {
            // No more events to read
        }
    }

    private TestEvent readEvent(InputStream in, String column, String query) throws IOException {
        int packetLength = readPacketHeader(in);
        if (packetLength == -1) {
            throw new RuntimeException("Error reading packet header");
        }

        // Consume event header
        byte[] eventHeader = readRawEventData(in, 19);

        int eventLength =
                (int)
                        PacketUtil.readLong(
                                new ByteArrayInputStream(Arrays.copyOfRange(eventHeader, 9, 13)),
                                4);

        int eventType =
                PacketUtil.readInt(
                        new ByteArrayInputStream(Arrays.copyOfRange(eventHeader, 4, 5)), 1);

        // Consume event body
        byte[] eventBody = readRawEventData(in, eventLength - 19 - checksumLength);

        // Consume checksum if present
        in.skip(checksumLength);

        return new TestEvent(eventType, eventHeader, eventBody, column, query);
    }

    private byte[] readRawEventData(InputStream in, int length) throws IOException {
        byte[] buffer = new byte[length];

        int numBytesRead = 0;
        for (int i = 0; i < length; i++) {
            buffer[i] = (byte) read(in);
            numBytesRead++;
        }

        if (numBytesRead != length)
            throw new RuntimeException("Bytes read does not match expected length");

        return buffer;
    }

    private byte[] readPayload(InputStream in, int length) throws IOException {
        byte[] buffer = new byte[length];
        int numBytesRead = in.read(buffer);
        if (numBytesRead != length)
            throw new RuntimeException("Bytes read does not match expected payload length");
        return buffer;
    }

    private int readPayloadLength(InputStream in) throws IOException {
        int result = 0;
        for (int i = 0; i < 3; ++i) {
            result |= (read(in) << (i << 3));
        }
        return result;
    }

    private int read(InputStream in) throws IOException {
        int result;
        if (peek == null) {
            result = in.read();
        } else {
            result = peek;
            peek = null;
        }
        return result;
    }

    private int peek(InputStream in) throws IOException {
        if (peek == null) {
            peek = in.read();
        }
        return peek;
    }

    private void execute(Connection conn, String... sql) {
        for (String s : sql) {
            try (Statement statement = conn.createStatement()) {
                statement.execute(s);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }
}

class QueryCommand {
    public static byte[] create(String sql) throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        PacketUtil.writeInt(buffer, CaptureBinlogEvents.CommandType.QUERY.ordinal(), 1);
        PacketUtil.writeString(buffer, sql);
        return buffer.toByteArray();
    }
}

class DumpBinaryLogCommand {
    public static byte[] create(long serverId, String binlogFilename, long binlogPosition)
            throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        PacketUtil.writeInt(buffer, CaptureBinlogEvents.CommandType.BINLOG_DUMP.ordinal(), 1);
        PacketUtil.writeLong(buffer, binlogPosition, 4);
        PacketUtil.writeInt(buffer, 0, 2); // flag
        PacketUtil.writeLong(buffer, serverId, 4);
        PacketUtil.writeString(buffer, binlogFilename);
        return buffer.toByteArray();
    }
}
