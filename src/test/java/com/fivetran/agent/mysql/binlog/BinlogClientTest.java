/**
* Copyright (c) Fivetran 2018
**/
package com.fivetran.agent.mysql.binlog;

import com.fivetran.agent.mysql.config.DatabaseCredentials;
import com.fivetran.agent.mysql.source.*;
import com.fivetran.agent.mysql.source.binlog.BinlogInputStream;
import com.fivetran.agent.mysql.source.binlog.client.BinlogClient;
import com.fivetran.agent.mysql.source.binlog.client.EventReader;
import com.fivetran.agent.mysql.source.binlog.client.PacketChannel;
import com.fivetran.agent.mysql.source.binlog.client.shyiko.Greeting;
import com.fivetran.agent.mysql.source.binlog.parser.TableMapEventBody;
import com.google.common.collect.ImmutableList;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.TimeoutException;

import static javax.xml.bind.DatatypeConverter.parseHexBinary;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class BinlogClientTest {

    private static BinlogClient client;

    @BeforeClass
    public static void beforeClass() {
        // todo: remove eventually
        client = new BinlogClient(
                new DatabaseCredentials(
                        "localhost",
                        3306,
                        "",
                        ""
                ));
    }

    @Test
    public void readSingleEvent() {
        TestBinlogClient testClient = new TestBinlogClient();
        LinkedList<SourceEvent> events = new LinkedList<>();

        BinlogPosition position = new BinlogPosition("binlog-file", 0);

        EventReader reader = testClient.events(position);
        SourceEvent sourceEvent;

        while ((sourceEvent = reader.readEvent()).event != SourceEventType.TIMEOUT) {
           events.add(sourceEvent);
        }

        assertThat(events.size(), equalTo(1));
        assertThat(events.pop(), equalTo(
                SourceEvent.createInsert(
                        new TableRef("schema", "table"),
                        new BinlogPosition("binlog-file", 26442),
                        ImmutableList.of(new Row("2147483647"))
                )));
    }

    private class MockChannel implements PacketChannel {

        BinlogInputStream in = new BinlogInputStream(
                parseHexBinary(
                        "0000000000D0AA835A1E01000000280000004A6700000000" +
                                "F700000000000100020001FFFEFFFFFF7F00000000"));

        @Override
        public byte[] readPacket() {
            return new byte[0];
        }

        @Override
        public void writePacket(byte[] packet, int packetNumber) {
        }

        @Override
        public byte[] read(int length) throws TimeoutException {
            if (in.available() < 1) {
                throw new TimeoutException();
            }
            return in.read(length);
        }

        @Override
        public List<String[]> queryForResultSet(String sql) {
            List<String[]> resultSet = new ArrayList<>();

            switch (sql) {
                case "show master status":
                    resultSet.add(new String[] {"binlog_test_file", "0"});
                    break;
                case "select @@server_id":
                    resultSet.add(new String[] {"1"});
                    break;
                case "show global variables like 'binlog_checksum'":
                    resultSet.add(new String[] {"unused", "CRC32"});
                    break;
                default:
                    throw new RuntimeException("Unimplemented sql query");
            }

            return resultSet;
        }

        @Override
        public void queryToSetVariable(String sql) {
        }

        @Override
        public void initializeBinlogDump(long serverId, BinlogPosition binlogPosition) {
        }

        @Override
        public Greeting getGreeting() {
            return null;
        }

        @Override
        public void writeGreetingResponse(String username, String password, int collation, String salt) {
        }

        @Override
        public boolean isOpen() {
            return false;
        }

        @Override
        public void close() {
        }
    }

    private class TestBinlogClient extends BinlogClient {

        TestBinlogClient() {
            super(
                    new DatabaseCredentials(
                            "test_host",
                            1001,
                            "fivetran",
                            "12345"));
            TableMapEventBody tableMapEventBody = new TableMapEventBody();
            tableMapEventBody.setTableRef(new TableRef("schema", "table"));
            tableMapEventBody.setColumnTypes(new byte[] {3});
            tableMapEventBody.setColumnMetadata(new int[] {0});
            this.tableMaps.put(247L, tableMapEventBody);
        }

        @Override
        public PacketChannel connect() {
            return new MockChannel();
        }

    }
}
