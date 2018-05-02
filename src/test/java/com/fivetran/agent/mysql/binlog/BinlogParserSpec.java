/**
* Copyright (c) Fivetran 2018
**/
package com.fivetran.agent.mysql.binlog;

import com.fivetran.agent.mysql.source.*;
import com.fivetran.agent.mysql.source.binlog.parser.*;
import com.google.common.collect.ImmutableList;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static com.fivetran.agent.mysql.source.binlog.parser.EventType.EXT_WRITE_ROWS;
import static com.fivetran.agent.mysql.source.binlog.parser.EventType.TABLE_MAP;
import static javax.xml.bind.DatatypeConverter.parseHexBinary;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.CoreMatchers.equalTo;

public class BinlogParserSpec {

    // todo: add test that updates/deletes multiple rows
    // todo: add test that makes multiple inserts in the same query
    //       - ex: INSERT INTO example
    //                (example_id, name, value, other_value)
    //             VALUES
    //                (100, 'Name 1', 'Value 1', 'Other 1'),
    //                (101, 'Name 2', 'Value 2', 'Other 2'),
    //                (102, 'Name 3', 'Value 3', 'Other 3'),
    //                (103, 'Name 4', 'Value 4', 'Other 4');

    /**
     * To generate test cases, refer to the CaptureBinlogEvents script
     */

    private EventHeaderParser headerParser = new EventHeaderParser();
    private EventBodyParser bodyParser = new EventBodyParser();
    private TableRef tableRef = new TableRef("capture_binlog_events", "foo");

    /**
     * Place tests generated in CaptureBinlogEvents below
     **/


    @Test
    public void readHeader() throws IOException {
        // Query: INSERT INTO capture_binlog_events.foo (readHeader) VALUES (2147483647)

        byte[] header = parseHexBinary("80AD835A1E0100000028000000BA6B00000000");

        EventHeader eventHeader = headerParser.parse(header);
        // assertThat(eventHeader.getTimestamp(), equalTo(calculate value)); // todo: manually calculate values
        assertThat(eventHeader.getType(), equalTo(EXT_WRITE_ROWS));
        assertThat(eventHeader.getServerId(), equalTo(1L));
        // assertThat(eventHeader.getEventLength(), equalTo(calculate value));
        // assertThat(eventHeader.getBodyLength(), equalTo(caluclate value));
        assertThat(eventHeader.getHeaderLength(), equalTo(19L));
        // assertThat(eventHeader.getNextPosition(), equalTo(calculate value));
        assertThat(eventHeader.getFlags(), equalTo(0));
    }

    @Test
    public void insertRow() throws IOException {
        // Query: INSERT INTO capture_binlog_events.foo (insertRow) VALUES ('[1, "a"]')

        Map<Long, TableMapEventBody> tableMaps = new HashMap<>();

        byte[] tableMapHex =
                parseHexBinary("FA0000000000010015636170747572655F62696E6C6F675F6576656E74730003666F6F0001F5010401");
        bodyParser.parse(tableMapHex, TABLE_MAP, tableMaps);

        byte[] modifyingEventHex =
                parseHexBinary("FA00000000000100020001FFFE0D0000000202000C000501000C0A000161");
        ModifyingEventBody modifyingEventBody = (ModifyingEventBody) bodyParser.parse(modifyingEventHex, EXT_WRITE_ROWS, tableMaps);
        assertThat(modifyingEventBody.getNewRows(), equalTo(ImmutableList.of(new Row("[1,\"a\"]"))));
    }

    @Test
    public void multiLongtext() throws IOException {
        // Query: INSERT INTO capture_binlog_events.foo (multiLongtext) VALUES ('foobarbazqux')

        Map<Long, TableMapEventBody> tableMaps = new HashMap<>();

        byte[] tableMapHex =
                parseHexBinary("1F0200000000010015636170747572655F62696E6C6F675F6576656E74730003666F6F0001FC010401");
        bodyParser.parse(tableMapHex, TABLE_MAP, tableMaps);

        byte[] modifyingEventHex =
                parseHexBinary("1F02000000000100020001FFFE0C000000666F6F62617262617A717578");
        ModifyingEventBody modifyingEventBody = (ModifyingEventBody) bodyParser.parse(modifyingEventHex, EXT_WRITE_ROWS, tableMaps);
        assertThat(modifyingEventBody.getNewRows(), equalTo(ImmutableList.of(new Row("foobarbazqux"))));
    }

    @Test
    public void complexArrayJson() throws IOException {
        // Query: INSERT INTO capture_binlog_events.foo (complexArrayJson) VALUES ('[1, "a"]')

        Map<Long, TableMapEventBody> tableMaps = new HashMap<>();

        byte[] tableMapHex =
                parseHexBinary("200200000000010015636170747572655F62696E6C6F675F6576656E74730003666F6F0001F5010401");
        TableMapEventBody tableMapEventBody = (TableMapEventBody) bodyParser.parse(tableMapHex, TABLE_MAP, tableMaps);

        byte[] modifyingEventHex =
                parseHexBinary("2002000000000100020001FFFE0D0000000202000C000501000C0A000161");
        ModifyingEventBody modifyingEventBody = (ModifyingEventBody) bodyParser.parse(modifyingEventHex, EXT_WRITE_ROWS, tableMaps);
        assertThat(modifyingEventBody.getNewRows(), equalTo(ImmutableList.of(new Row("[1,\"a\"]"))));
    }

    @Test
    public void nestedArray2Json() throws IOException {
        // Query: INSERT INTO capture_binlog_events.foo (nestedArray2Json) VALUES ('[{"a":1}]')

        Map<Long, TableMapEventBody> tableMaps = new HashMap<>();

        byte[] tableMapHex =
                parseHexBinary("210200000000010015636170747572655F62696E6C6F675F6576656E74730003666F6F0001F5010401");
        bodyParser.parse(tableMapHex, TABLE_MAP, tableMaps);

        byte[] modifyingEventHex =
                parseHexBinary("2102000000000100020001FFFE14000000020100130000070001000C000B00010005010061");
        ModifyingEventBody modifyingEventBody = (ModifyingEventBody) bodyParser.parse(modifyingEventHex, EXT_WRITE_ROWS, tableMaps);
        assertThat(modifyingEventBody.getNewRows(), equalTo(ImmutableList.of(new Row("[{\"a\":1}]"))));
    }
}

