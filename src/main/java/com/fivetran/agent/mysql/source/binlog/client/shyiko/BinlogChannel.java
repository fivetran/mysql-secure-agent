/**
* Copyright (c) Fivetran 2018
**/
/*
 * Copyright 2013 Stanley Shyiko
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.fivetran.agent.mysql.source.binlog.client.shyiko;

import com.fivetran.agent.mysql.binlog_test_generator.ClientCapabilities;
import com.fivetran.agent.mysql.source.BinlogPosition;
import com.fivetran.agent.mysql.source.binlog.BinlogInputStream;
import com.fivetran.agent.mysql.source.binlog.BinlogOutputStream;
import com.fivetran.agent.mysql.source.binlog.client.PacketChannel;
import com.fivetran.agent.mysql.source.binlog.client.PacketReadException;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.fivetran.agent.mysql.source.binlog.client.BinlogClient.*;
import static com.fivetran.agent.mysql.source.binlog.client.BinlogClient.FOUR_BYTES;

public class BinlogChannel implements PacketChannel {

    private static final int MAX_PACKET_LENGTH = 4;
    private static final int MIN_PACKET_LENGTH = 1;
    private static final int READ_TIMEOUT = 1000;  // TODO this will eventually be 5 minutes

    private AsynchronousSocketChannel channel;

    public BinlogChannel(AsynchronousSocketChannel channel) {
        this.channel = channel;
    }

    @Override
    public byte[] readPacket() throws TimeoutException {
        byte[] packetHeader = read(FOUR_BYTES);
        BinlogInputStream in = new BinlogInputStream(packetHeader);

        int packetLength = in.readInteger(THREE_BYTES);
        in.skip(ONE_BYTE); // skip packet number

        if ((packetLength & 0xFF) == 0xFF) {
            throw new RuntimeException("Server Exception");
        }
        return read(packetLength);
    }

    @Override
    public byte[] read(int length) throws TimeoutException {
        try {
            ByteBuffer buffer = ByteBuffer.allocate(length);
            Future<Integer> read = channel.read(buffer);

            Integer numBytesRead = read.get(READ_TIMEOUT, TimeUnit.MILLISECONDS);
            if (numBytesRead != length)
                throw new RuntimeException("Bytes read does not match expected payload length");

            return buffer.array();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void writePacket(byte[] packet, int packetNumber) {
        BinlogOutputStream out = new BinlogOutputStream();
        out.writeInteger(packet.length, THREE_BYTES);
        out.writeInteger(packetNumber, ONE_BYTE);
        out.write(packet, 0, packet.length);
        channel.write(ByteBuffer.wrap(out.toByteArray()));
    }

    @Override
    public void queryToSetVariable(String sql) throws IOException, TimeoutException {
        writeQuery(sql);
        if (packetReadFailed(readPacket())) {
            throw new PacketReadException("Query response packet could not be read");
        }
    }

    @Override
    public List<String[]> queryForResultSet(String sql) throws IOException, TimeoutException {
        writeQuery(sql);
        return readResultSet();
    }

    private void writeQuery(String sql) throws IOException {
        BinlogOutputStream buffer = new BinlogOutputStream();

        buffer.writeInteger(CommandType.QUERY.ordinal(), ONE_BYTE);
        buffer.writeString(sql);

        writePacket(buffer.toByteArray(), 0);
    }

    private List<String[]> readResultSet() throws IOException, TimeoutException {
        while (isNotEOF(readPacket())) ; /* skip until eof_packet */

        List<String[]> resultSet = new ArrayList<>();
        for (byte[] bytes; isNotEOF(bytes = readPacket()); ) {
            resultSet.add(readResultSetRow(bytes));
        }
        return resultSet;
    }

    private String[] readResultSetRow(byte[] bytes) throws IOException {
        BinlogInputStream buffer = new BinlogInputStream(bytes);
        List<String> values = new ArrayList<>();
        while (buffer.available() > 0) {
            values.add(readLengthEncodedString(buffer));
        }
        return values.toArray(new String[values.size()]);
    }

    private static String readLengthEncodedString(BinlogInputStream buffer) throws IOException {
        int length = buffer.readPackedInteger();

        byte[] characters = new byte[length];
        if (length > 0) {
            int numBytesRead = buffer.read(characters);
            if (numBytesRead != length)
                throw new RuntimeException("Bytes read does not match expected payload length");
        }
        return new String(characters);
    }

    @Override
    public void initializeBinlogDump(long serverId, BinlogPosition binlogPosition) throws IOException {
        BinlogOutputStream buffer = new BinlogOutputStream();

        buffer.writeInteger(CommandType.BINLOG_DUMP.ordinal(), ONE_BYTE);
        buffer.writeLong(binlogPosition.position, FOUR_BYTES);
        buffer.writeInteger(0, TWO_BYTES); // flag
        buffer.writeLong(serverId, FOUR_BYTES);
        buffer.writeString(binlogPosition.file);

        writePacket(buffer.toByteArray(), 0);
    }

    @Override
    public Greeting getGreeting() throws IOException, TimeoutException {
        byte[] packet = readPacket();
        return new Greeting(packet);
    }

    @Override
    public void writeGreetingResponse(String username, String password, int collation, String salt) throws IOException, TimeoutException {
        BinlogOutputStream buffer = new BinlogOutputStream();
        byte[] passwordSHA1 = "".equals(password) ? new byte[0] : passwordCompatibleWithMySQL411(password, salt);
        int clientCapabilities = ClientCapabilities.LONG_FLAG | ClientCapabilities.PROTOCOL_41;

        buffer.writeInteger(clientCapabilities, MAX_PACKET_LENGTH);
        buffer.writeInteger(0, MAX_PACKET_LENGTH);
        buffer.writeInteger(collation, MIN_PACKET_LENGTH);  // TODO try to find collation codes for utf 8

        for (int i = 0; i < 23; i++) {
            buffer.write(0);
        }

        buffer.writeString(username);
        buffer.writeInteger(passwordSHA1.length, MIN_PACKET_LENGTH);
        buffer.write(passwordSHA1);

        writePacket(buffer.toByteArray(), 1);

        if (packetReadFailed(readPacket())) {
            throw new PacketReadException("Greeting response was not accepted");
        }
    }

    private byte[] passwordCompatibleWithMySQL411(String password, String salt) {
        MessageDigest sha;
        try {
            sha = MessageDigest.getInstance("SHA-1");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
        byte[] passwordHash = sha.digest(password.getBytes());
        return xor(passwordHash, sha.digest(union(salt.getBytes(), sha.digest(passwordHash))));
    }

    private boolean isNotEOF(byte[] packet) {
        return packet[0] != (byte) 0xFE;
    }

    private boolean packetReadFailed(byte[] packet) {
        return packet[0] != (byte) 0x00;
    }

    private byte[] union(byte[] a, byte[] b) {
        byte[] r = new byte[a.length + b.length];
        System.arraycopy(a, 0, r, 0, a.length);
        System.arraycopy(b, 0, r, a.length, b.length);
        return r;
    }

    private byte[] xor(byte[] a, byte[] b) {
        byte[] r = new byte[a.length];
        for (int i = 0; i < r.length; i++) {
            r[i] = (byte) (a[i] ^ b[i]);
        }
        return r;
    }

    @Override
    public boolean isOpen() {
        return channel.isOpen();
    }

    @Override
    public void close() throws IOException {
        channel.close();
    }
}

