package com.fivetran.agent.mysql.source.binlog.client;

import com.fivetran.agent.mysql.source.BinlogPosition;
import com.fivetran.agent.mysql.source.binlog.client.shyiko.Greeting;

import java.io.IOException;
import java.nio.channels.Channel;
import java.util.List;
import java.util.concurrent.TimeoutException;

public interface PacketChannel extends Channel {
    /**
     * Reads from the channel until a byte array of size <code>length</code> is filled.
     */
    byte[] read(int length) throws TimeoutException;

    /**
     * Reads from the channel a packet in the following format:
     * 3 bytes               - packet length
     * 1 byte                - packet number
     * [packet length] bytes - packet content
     *
     * Returns the packet content as a byte array.
     */
    byte[] readPacket() throws IOException, TimeoutException;

    /**
     * Reads an initial handshake packet from the channel and parses it as a Greeting.
     *
     * See {@https://dev.mysql.com/doc/internals/en/connection-phase-packets.html#packet-Protocol::Handshake}
     */
    Greeting getGreeting() throws IOException, TimeoutException;

    /**
     * Writes a handshake protocol response to the channel and confirms the server responded with an OK packet.
     *
     * See {@https://dev.mysql.com/doc/internals/en/connection-phase-packets.html#packet-Protocol::HandshakeResponse
     */
    void writeGreetingResponse(String username, String password, int collation, String salt) throws IOException, TimeoutException;

    /**
     * Writes a packet to the channel in the following format:
     * 3 bytes               - packet length
     * 1 byte                - packet number
     * [packet length] bytes - packet content
     */
    void writePacket(byte[] packet, int packetNumber) throws IOException;

    /**
     * Writes the <code>sql</code> query to the channel and returns the result set
     */
    List<String[]> queryForResultSet(String sql) throws IOException, TimeoutException;

    /**
     * Writes the <code>sql</code> query to the channel with the intention of setting a variable on the connected
     * MySql server. The response set is ignored.
     */
    void queryToSetVariable(String sql) throws IOException, TimeoutException;

    /**
     * Writes the binary log dump command to the channel in the following format:
     * 1 byte                 - binary log dump command
     * 4 bytes                - binary log position
     * 2 bytes                - optional flag
     * 4 bytes                - server id
     * [string length]        - binary log file
     * */
    void initializeBinlogDump(long serverId, BinlogPosition binlogPosition) throws IOException;
}
