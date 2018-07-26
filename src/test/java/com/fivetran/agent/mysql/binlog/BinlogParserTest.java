/**
 * Copyright (c) Fivetran 2018
 **/
package com.fivetran.agent.mysql.binlog;

import com.fivetran.agent.mysql.source.*;
import com.fivetran.agent.mysql.source.binlog.parser.*;
import com.google.common.collect.ImmutableList;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.fivetran.agent.mysql.source.binlog.parser.EventType.*;
import static javax.xml.bind.DatatypeConverter.parseHexBinary;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.CoreMatchers.equalTo;

public class BinlogParserTest {

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
        assertThat(eventHeader.getServerId(), equalTo(1));
        // assertThat(eventHeader.getEventLength(), equalTo(calculate value));
        // assertThat(eventHeader.getBodyLength(), equalTo(caluclate value));
        assertThat(eventHeader.getHeaderLength(), equalTo(19));
        // assertThat(eventHeader.getNextPosition(), equalTo(calculate value));
        assertThat(eventHeader.getFlags(), equalTo(0));
    }

    @Test
    public void readTableMap() throws IOException {
        // Query: INSERT INTO capture_binlog_events.foo (readTableMap) VALUES (2147483647)

        Map<Long, TableMapEventBody> tableMaps = new HashMap<>();

        byte[] tableMapHex =
                parseHexBinary("F70000000000010015636170747572655F62696E6C6F675F6576656E74730003666F6F0001030001");
        bodyParser.parse(tableMapHex, TABLE_MAP, tableMaps);

        assertThat(tableMaps.get(247L).getTableRef().schema, equalTo("capture_binlog_events"));
        assertThat(tableMaps.get(247L).getTableRef().name, equalTo("foo"));
        assertThat(tableMaps.get(247L).getColumnTypes(), equalTo(new byte[]{3}));
    }

    @Test
    public void insertRow() throws IOException {
        // Query: INSERT INTO capture_binlog_events.foo (insertRow) VALUES ('[1, "a"]')

        Map<Long, TableMapEventBody> tableMaps = new HashMap<>();

        byte[] tableMapHex =
                parseHexBinary("110100000000010015636170747572655F62696E6C6F675F6576656E74730003666F6F0001F5010401");
        bodyParser.parse(tableMapHex, TABLE_MAP, tableMaps);

        byte[] modifyingEventHex =
                parseHexBinary("1101000000000100020001FFFE0D0000000202000C000501000C0A000161");
        ModifyingEventBody modifyingEventBody = (ModifyingEventBody) bodyParser.parse(modifyingEventHex, EXT_WRITE_ROWS, tableMaps);
        assertThat(modifyingEventBody.getNewRows(), equalTo(ImmutableList.of(new Row("[1,\"a\"]"))));
    }

    @Test
    public void updateRow() throws IOException {
        // Query: UPDATE capture_binlog_events.foo SET updateRow = 'xuqzabraboof'

        Map<Long, TableMapEventBody> tableMaps = new HashMap<>();

        byte[] tableMapHex =
                parseHexBinary("120100000000010015636170747572655F62696E6C6F675F6576656E74730003666F6F00010F024B0001");
        bodyParser.parse(tableMapHex, TABLE_MAP, tableMaps);

        byte[] modifyingEventHex =
                parseHexBinary("1201000000000100020001FFFFFE0C666F6F62617262617A717578FE0C7875717A61627261626F6F66");
        ModifyingEventBody modifyingEventBody = (ModifyingEventBody) bodyParser.parse(modifyingEventHex, EXT_UPDATE_ROWS, tableMaps);
        assertThat(modifyingEventBody.getNewRows(), equalTo(ImmutableList.of(new Row("xuqzabraboof"))));
    }

    @Test
    public void deleteRow() throws IOException {
        // Query: DELETE FROM capture_binlog_events.foo

        Map<Long, TableMapEventBody> tableMaps = new HashMap<>();

        byte[] tableMapHex =
                parseHexBinary("130100000000010015636170747572655F62696E6C6F675F6576656E74730003666F6F0001090001");
        bodyParser.parse(tableMapHex, TABLE_MAP, tableMaps);

        byte[] modifyingEventHex =
                parseHexBinary("1301000000000100020001FFFEFFFF7F");
        ModifyingEventBody modifyingEventBody = (ModifyingEventBody) bodyParser.parse(modifyingEventHex, EXT_DELETE_ROWS, tableMaps);
        assertThat(modifyingEventBody.getNewRows(), equalTo(ImmutableList.of(new Row("8388607"))));
    }

    @Test
    public void multiByte() throws IOException {
        // Query: INSERT INTO capture_binlog_events.foo (multiByte) VALUES ('aÃbڅcㅙdソe')

        Map<Long, TableMapEventBody> tableMaps = new HashMap<>();

        byte[] tableMapHex =
                parseHexBinary("140100000000010015636170747572655F62696E6C6F675F6576656E74730003666F6F00010F024B0001");
        bodyParser.parse(tableMapHex, TABLE_MAP, tableMaps);

        byte[] modifyingEventHex =
                parseHexBinary("1401000000000100020001FFFE0F61C38362DA8563E3859964E382BD65");
        ModifyingEventBody modifyingEventBody = (ModifyingEventBody) bodyParser.parse(modifyingEventHex, EXT_WRITE_ROWS, tableMaps);
        assertThat(modifyingEventBody.getNewRows(), equalTo(ImmutableList.of(new Row("aÃbڅcㅙdソe"))));
    }

    // TODO investigate whether we can sync unique charsets
    @Ignore
    @Test
    public void big5Charset() throws IOException {
        // Query: INSERT INTO capture_binlog_events.foo (big5Charset) VALUES ('什')

        Map<Long, TableMapEventBody> tableMaps = new HashMap<>();

        byte[] tableMapHex =
                parseHexBinary("150100000000010015636170747572655F62696E6C6F675F6576656E74730003666F6F0001FE02FE0201");
        bodyParser.parse(tableMapHex, TABLE_MAP, tableMaps);

        byte[] modifyingEventHex =
                parseHexBinary("1501000000000100020001FFFE02A4B0");
        ModifyingEventBody modifyingEventBody = (ModifyingEventBody) bodyParser.parse(modifyingEventHex, EXT_WRITE_ROWS, tableMaps);
        assertThat(modifyingEventBody.getNewRows(), equalTo(ImmutableList.of(new Row("什"))));
    }

    @Ignore
    @Test
    public void cp850Charset() throws IOException {
        // Query: INSERT INTO capture_binlog_events.foo (cp850Charset) VALUES ('Ç')

        Map<Long, TableMapEventBody> tableMaps = new HashMap<>();

        byte[] tableMapHex =
                parseHexBinary("160100000000010015636170747572655F62696E6C6F675F6576656E74730003666F6F0001FE02FE0101");
        bodyParser.parse(tableMapHex, TABLE_MAP, tableMaps);

        byte[] modifyingEventHex =
                parseHexBinary("1601000000000100020001FFFE0180");
        ModifyingEventBody modifyingEventBody = (ModifyingEventBody) bodyParser.parse(modifyingEventHex, EXT_WRITE_ROWS, tableMaps);
        assertThat(modifyingEventBody.getNewRows(), equalTo(ImmutableList.of(new Row("Ç"))));
    }

    @Ignore
    @Test
    public void koi8rCharset() throws IOException {
        // Query: INSERT INTO capture_binlog_events.foo (koi8rCharset) VALUES ('ж')

        Map<Long, TableMapEventBody> tableMaps = new HashMap<>();

        byte[] tableMapHex =
                parseHexBinary("170100000000010015636170747572655F62696E6C6F675F6576656E74730003666F6F0001FE02FE0101");
        bodyParser.parse(tableMapHex, TABLE_MAP, tableMaps);

        byte[] modifyingEventHex =
                parseHexBinary("1701000000000100020001FFFE01D6");
        ModifyingEventBody modifyingEventBody = (ModifyingEventBody) bodyParser.parse(modifyingEventHex, EXT_WRITE_ROWS, tableMaps);
        assertThat(modifyingEventBody.getNewRows(), equalTo(ImmutableList.of(new Row("ж"))));
    }

    @Ignore
    @Test
    public void utf8Charset() throws IOException {
        // Query: INSERT INTO capture_binlog_events.foo (utf8Charset) VALUES ('≈')

        Map<Long, TableMapEventBody> tableMaps = new HashMap<>();

        byte[] tableMapHex =
                parseHexBinary("180100000000010015636170747572655F62696E6C6F675F6576656E74730003666F6F0001FE02FE0301");
        bodyParser.parse(tableMapHex, TABLE_MAP, tableMaps);

        byte[] modifyingEventHex =
                parseHexBinary("1801000000000100020001FFFE03E28988");
        ModifyingEventBody modifyingEventBody = (ModifyingEventBody) bodyParser.parse(modifyingEventHex, EXT_WRITE_ROWS, tableMaps);
        assertThat(modifyingEventBody.getNewRows(), equalTo(ImmutableList.of(new Row("≈"))));
    }

    @Ignore
    @Test
    public void latin1Charset() throws IOException {
        // Query: INSERT INTO capture_binlog_events.foo (latin1Charset) VALUES ('Ð')

        Map<Long, TableMapEventBody> tableMaps = new HashMap<>();

        byte[] tableMapHex =
                parseHexBinary("190100000000010015636170747572655F62696E6C6F675F6576656E74730003666F6F0001FE02FE0101");
        bodyParser.parse(tableMapHex, TABLE_MAP, tableMaps);

        byte[] modifyingEventHex =
                parseHexBinary("1901000000000100020001FFFE01D0");
        ModifyingEventBody modifyingEventBody = (ModifyingEventBody) bodyParser.parse(modifyingEventHex, EXT_WRITE_ROWS, tableMaps);
        assertThat(modifyingEventBody.getNewRows(), equalTo(ImmutableList.of(new Row("Ð"))));
    }

    @Ignore
    @Test
    public void latin2Charset() throws IOException {
        // Query: INSERT INTO capture_binlog_events.foo (latin2Charset) VALUES ('ö')

        Map<Long, TableMapEventBody> tableMaps = new HashMap<>();

        byte[] tableMapHex =
                parseHexBinary("1A0100000000010015636170747572655F62696E6C6F675F6576656E74730003666F6F0001FE02FE0101");
        bodyParser.parse(tableMapHex, TABLE_MAP, tableMaps);

        byte[] modifyingEventHex =
                parseHexBinary("1A01000000000100020001FFFE01F6");
        ModifyingEventBody modifyingEventBody = (ModifyingEventBody) bodyParser.parse(modifyingEventHex, EXT_WRITE_ROWS, tableMaps);
        assertThat(modifyingEventBody.getNewRows(), equalTo(ImmutableList.of(new Row("ö"))));
    }

    @Ignore
    @Test
    public void asciiCharset() throws IOException {
        // Query: INSERT INTO capture_binlog_events.foo (asciiCharset) VALUES ('')

        Map<Long, TableMapEventBody> tableMaps = new HashMap<>();

        byte[] tableMapHex =
                parseHexBinary("1B0100000000010015636170747572655F62696E6C6F675F6576656E74730003666F6F0001FE02FE0101");
        bodyParser.parse(tableMapHex, TABLE_MAP, tableMaps);

        byte[] modifyingEventHex =
                parseHexBinary("1B01000000000100020001FFFE011A");
        ModifyingEventBody modifyingEventBody = (ModifyingEventBody) bodyParser.parse(modifyingEventHex, EXT_WRITE_ROWS, tableMaps);
        assertThat(modifyingEventBody.getNewRows(), equalTo(ImmutableList.of(new Row(""))));
    }

    @Ignore
    @Test
    public void ujisCharset() throws IOException {
        // Query: INSERT INTO capture_binlog_events.foo (ujisCharset) VALUES ('ｹ')

        Map<Long, TableMapEventBody> tableMaps = new HashMap<>();

        byte[] tableMapHex =
                parseHexBinary("1C0100000000010015636170747572655F62696E6C6F675F6576656E74730003666F6F0001FE02FE0301");
        bodyParser.parse(tableMapHex, TABLE_MAP, tableMaps);

        byte[] modifyingEventHex =
                parseHexBinary("1C01000000000100020001FFFE028EB9");
        ModifyingEventBody modifyingEventBody = (ModifyingEventBody) bodyParser.parse(modifyingEventHex, EXT_WRITE_ROWS, tableMaps);
        assertThat(modifyingEventBody.getNewRows(), equalTo(ImmutableList.of(new Row("ｹ"))));
    }

    @Ignore
    @Test
    public void sjisCharset() throws IOException {
        // Query: INSERT INTO capture_binlog_events.foo (sjisCharset) VALUES ('ボ')

        Map<Long, TableMapEventBody> tableMaps = new HashMap<>();

        byte[] tableMapHex =
                parseHexBinary("1D0100000000010015636170747572655F62696E6C6F675F6576656E74730003666F6F0001FE02FE0201");
        bodyParser.parse(tableMapHex, TABLE_MAP, tableMaps);

        byte[] modifyingEventHex =
                parseHexBinary("1D01000000000100020001FFFE02837B");
        ModifyingEventBody modifyingEventBody = (ModifyingEventBody) bodyParser.parse(modifyingEventHex, EXT_WRITE_ROWS, tableMaps);
        assertThat(modifyingEventBody.getNewRows(), equalTo(ImmutableList.of(new Row("ボ"))));
    }

    @Ignore
    @Test
    public void hebrewCharset() throws IOException {
        // Query: INSERT INTO capture_binlog_events.foo (hebrewCharset) VALUES ('ה')

        Map<Long, TableMapEventBody> tableMaps = new HashMap<>();

        byte[] tableMapHex =
                parseHexBinary("1E0100000000010015636170747572655F62696E6C6F675F6576656E74730003666F6F0001FE02FE0101");
        bodyParser.parse(tableMapHex, TABLE_MAP, tableMaps);

        byte[] modifyingEventHex =
                parseHexBinary("1E01000000000100020001FFFE01E4");
        ModifyingEventBody modifyingEventBody = (ModifyingEventBody) bodyParser.parse(modifyingEventHex, EXT_WRITE_ROWS, tableMaps);
        assertThat(modifyingEventBody.getNewRows(), equalTo(ImmutableList.of(new Row("ה"))));
    }

    @Ignore
    @Test
    public void tis620Charset() throws IOException {
        // Query: INSERT INTO capture_binlog_events.foo (tis620Charset) VALUES ('ฬ')

        Map<Long, TableMapEventBody> tableMaps = new HashMap<>();

        byte[] tableMapHex =
                parseHexBinary("1F0100000000010015636170747572655F62696E6C6F675F6576656E74730003666F6F0001FE02FE0101");
        bodyParser.parse(tableMapHex, TABLE_MAP, tableMaps);

        byte[] modifyingEventHex =
                parseHexBinary("1F01000000000100020001FFFE01CC");
        ModifyingEventBody modifyingEventBody = (ModifyingEventBody) bodyParser.parse(modifyingEventHex, EXT_WRITE_ROWS, tableMaps);
        assertThat(modifyingEventBody.getNewRows(), equalTo(ImmutableList.of(new Row("ฬ"))));
    }

    @Ignore
    @Test
    public void euckrCharset() throws IOException {
        // Query: INSERT INTO capture_binlog_events.foo (euckrCharset) VALUES ('ㅝ')

        Map<Long, TableMapEventBody> tableMaps = new HashMap<>();

        byte[] tableMapHex =
                parseHexBinary("200100000000010015636170747572655F62696E6C6F675F6576656E74730003666F6F0001FE02FE0201");
        bodyParser.parse(tableMapHex, TABLE_MAP, tableMaps);

        byte[] modifyingEventHex =
                parseHexBinary("2001000000000100020001FFFE02A4CD");
        ModifyingEventBody modifyingEventBody = (ModifyingEventBody) bodyParser.parse(modifyingEventHex, EXT_WRITE_ROWS, tableMaps);
        assertThat(modifyingEventBody.getNewRows(), equalTo(ImmutableList.of(new Row("ㅝ"))));
    }

    @Ignore
    @Test
    public void koi8uCharset() throws IOException {
        // Query: INSERT INTO capture_binlog_events.foo (koi8uCharset) VALUES ('й')

        Map<Long, TableMapEventBody> tableMaps = new HashMap<>();

        byte[] tableMapHex =
                parseHexBinary("210100000000010015636170747572655F62696E6C6F675F6576656E74730003666F6F0001FE02FE0101");
        bodyParser.parse(tableMapHex, TABLE_MAP, tableMaps);

        byte[] modifyingEventHex =
                parseHexBinary("2101000000000100020001FFFE01CA");
        ModifyingEventBody modifyingEventBody = (ModifyingEventBody) bodyParser.parse(modifyingEventHex, EXT_WRITE_ROWS, tableMaps);
        assertThat(modifyingEventBody.getNewRows(), equalTo(ImmutableList.of(new Row("й"))));
    }

    @Ignore
    @Test
    public void gb2312Charset() throws IOException {
        // Query: INSERT INTO capture_binlog_events.foo (gb2312Charset) VALUES ('与')

        Map<Long, TableMapEventBody> tableMaps = new HashMap<>();

        byte[] tableMapHex =
                parseHexBinary("220100000000010015636170747572655F62696E6C6F675F6576656E74730003666F6F0001FE02FE0201");
        bodyParser.parse(tableMapHex, TABLE_MAP, tableMaps);

        byte[] modifyingEventHex =
                parseHexBinary("2201000000000100020001FFFE02D3EB");
        ModifyingEventBody modifyingEventBody = (ModifyingEventBody) bodyParser.parse(modifyingEventHex, EXT_WRITE_ROWS, tableMaps);
        assertThat(modifyingEventBody.getNewRows(), equalTo(ImmutableList.of(new Row("与"))));
    }

    @Ignore
    @Test
    public void greekCharset() throws IOException {
        // Query: INSERT INTO capture_binlog_events.foo (greekCharset) VALUES ('ζ')

        Map<Long, TableMapEventBody> tableMaps = new HashMap<>();

        byte[] tableMapHex =
                parseHexBinary("230100000000010015636170747572655F62696E6C6F675F6576656E74730003666F6F0001FE02FE0101");
        bodyParser.parse(tableMapHex, TABLE_MAP, tableMaps);

        byte[] modifyingEventHex =
                parseHexBinary("2301000000000100020001FFFE01E6");
        ModifyingEventBody modifyingEventBody = (ModifyingEventBody) bodyParser.parse(modifyingEventHex, EXT_WRITE_ROWS, tableMaps);
        assertThat(modifyingEventBody.getNewRows(), equalTo(ImmutableList.of(new Row("ζ"))));
    }

    @Ignore
    @Test
    public void cp1250Charset() throws IOException {
        // Query: INSERT INTO capture_binlog_events.foo (cp1250Charset) VALUES ('ß')

        Map<Long, TableMapEventBody> tableMaps = new HashMap<>();

        byte[] tableMapHex =
                parseHexBinary("240100000000010015636170747572655F62696E6C6F675F6576656E74730003666F6F0001FE02FE0101");
        bodyParser.parse(tableMapHex, TABLE_MAP, tableMaps);

        byte[] modifyingEventHex =
                parseHexBinary("2401000000000100020001FFFE01DF");
        ModifyingEventBody modifyingEventBody = (ModifyingEventBody) bodyParser.parse(modifyingEventHex, EXT_WRITE_ROWS, tableMaps);
        assertThat(modifyingEventBody.getNewRows(), equalTo(ImmutableList.of(new Row("ß"))));
    }

    @Ignore
    @Test
    public void gbkCharset() throws IOException {
        // Query: INSERT INTO capture_binlog_events.foo (gbkCharset) VALUES ('堃')

        Map<Long, TableMapEventBody> tableMaps = new HashMap<>();

        byte[] tableMapHex =
                parseHexBinary("250100000000010015636170747572655F62696E6C6F675F6576656E74730003666F6F0001FE02FE0201");
        bodyParser.parse(tableMapHex, TABLE_MAP, tableMaps);

        byte[] modifyingEventHex =
                parseHexBinary("2501000000000100020001FFFE0288D2");
        ModifyingEventBody modifyingEventBody = (ModifyingEventBody) bodyParser.parse(modifyingEventHex, EXT_WRITE_ROWS, tableMaps);
        assertThat(modifyingEventBody.getNewRows(), equalTo(ImmutableList.of(new Row("堃"))));
    }

    @Ignore
    @Test
    public void latin5Charset() throws IOException {
        // Query: INSERT INTO capture_binlog_events.foo (latin5Charset) VALUES ('Æ')

        Map<Long, TableMapEventBody> tableMaps = new HashMap<>();

        byte[] tableMapHex =
                parseHexBinary("260100000000010015636170747572655F62696E6C6F675F6576656E74730003666F6F0001FE02FE0101");
        bodyParser.parse(tableMapHex, TABLE_MAP, tableMaps);

        byte[] modifyingEventHex =
                parseHexBinary("2601000000000100020001FFFE01C6");
        ModifyingEventBody modifyingEventBody = (ModifyingEventBody) bodyParser.parse(modifyingEventHex, EXT_WRITE_ROWS, tableMaps);
        assertThat(modifyingEventBody.getNewRows(), equalTo(ImmutableList.of(new Row("Æ"))));
    }

    @Ignore
    @Test
    public void ucs2Charset() throws IOException {
        // Query: INSERT INTO capture_binlog_events.foo (ucs2Charset) VALUES ('Ը')

        Map<Long, TableMapEventBody> tableMaps = new HashMap<>();

        byte[] tableMapHex =
                parseHexBinary("270100000000010015636170747572655F62696E6C6F675F6576656E74730003666F6F0001FE02FE0201");
        bodyParser.parse(tableMapHex, TABLE_MAP, tableMaps);

        byte[] modifyingEventHex =
                parseHexBinary("2701000000000100020001FFFE020538");
        ModifyingEventBody modifyingEventBody = (ModifyingEventBody) bodyParser.parse(modifyingEventHex, EXT_WRITE_ROWS, tableMaps);
        assertThat(modifyingEventBody.getNewRows(), equalTo(ImmutableList.of(new Row("Ը"))));
    }

    @Ignore
    @Test
    public void cp866Charset() throws IOException {
        // Query: INSERT INTO capture_binlog_events.foo (cp866Charset) VALUES ('Є')

        Map<Long, TableMapEventBody> tableMaps = new HashMap<>();

        byte[] tableMapHex =
                parseHexBinary("280100000000010015636170747572655F62696E6C6F675F6576656E74730003666F6F0001FE02FE0101");
        bodyParser.parse(tableMapHex, TABLE_MAP, tableMaps);

        byte[] modifyingEventHex =
                parseHexBinary("2801000000000100020001FFFE01F2");
        ModifyingEventBody modifyingEventBody = (ModifyingEventBody) bodyParser.parse(modifyingEventHex, EXT_WRITE_ROWS, tableMaps);
        assertThat(modifyingEventBody.getNewRows(), equalTo(ImmutableList.of(new Row("Є"))));
    }

    @Ignore
    @Test
    public void macceCharset() throws IOException {
        // Query: INSERT INTO capture_binlog_events.foo (macceCharset) VALUES ('◊')

        Map<Long, TableMapEventBody> tableMaps = new HashMap<>();

        byte[] tableMapHex =
                parseHexBinary("290100000000010015636170747572655F62696E6C6F675F6576656E74730003666F6F0001FE02FE0101");
        bodyParser.parse(tableMapHex, TABLE_MAP, tableMaps);

        byte[] modifyingEventHex =
                parseHexBinary("2901000000000100020001FFFE01D7");
        ModifyingEventBody modifyingEventBody = (ModifyingEventBody) bodyParser.parse(modifyingEventHex, EXT_WRITE_ROWS, tableMaps);
        assertThat(modifyingEventBody.getNewRows(), equalTo(ImmutableList.of(new Row("◊"))));
    }

    @Ignore
    @Test
    public void macromanCharset() throws IOException {
        // Query: INSERT INTO capture_binlog_events.foo (macromanCharset) VALUES ('€')

        Map<Long, TableMapEventBody> tableMaps = new HashMap<>();

        byte[] tableMapHex =
                parseHexBinary("2A0100000000010015636170747572655F62696E6C6F675F6576656E74730003666F6F0001FE02FE0101");
        bodyParser.parse(tableMapHex, TABLE_MAP, tableMaps);

        byte[] modifyingEventHex =
                parseHexBinary("2A01000000000100020001FFFE01DB");
        ModifyingEventBody modifyingEventBody = (ModifyingEventBody) bodyParser.parse(modifyingEventHex, EXT_WRITE_ROWS, tableMaps);
        assertThat(modifyingEventBody.getNewRows(), equalTo(ImmutableList.of(new Row("€"))));
    }

    @Ignore
    @Test
    public void cp852Charset() throws IOException {
        // Query: INSERT INTO capture_binlog_events.foo (cp852Charset) VALUES ('š')

        Map<Long, TableMapEventBody> tableMaps = new HashMap<>();

        byte[] tableMapHex =
                parseHexBinary("2B0100000000010015636170747572655F62696E6C6F675F6576656E74730003666F6F0001FE02FE0101");
        bodyParser.parse(tableMapHex, TABLE_MAP, tableMaps);

        byte[] modifyingEventHex =
                parseHexBinary("2B01000000000100020001FFFE01E7");
        ModifyingEventBody modifyingEventBody = (ModifyingEventBody) bodyParser.parse(modifyingEventHex, EXT_WRITE_ROWS, tableMaps);
        assertThat(modifyingEventBody.getNewRows(), equalTo(ImmutableList.of(new Row("š"))));
    }

    @Ignore
    @Test
    public void latin7Charset() throws IOException {
        // Query: INSERT INTO capture_binlog_events.foo (latin7Charset) VALUES ('Ų')

        Map<Long, TableMapEventBody> tableMaps = new HashMap<>();

        byte[] tableMapHex =
                parseHexBinary("2C0100000000010015636170747572655F62696E6C6F675F6576656E74730003666F6F0001FE02FE0101");
        bodyParser.parse(tableMapHex, TABLE_MAP, tableMaps);

        byte[] modifyingEventHex =
                parseHexBinary("2C01000000000100020001FFFE01D8");
        ModifyingEventBody modifyingEventBody = (ModifyingEventBody) bodyParser.parse(modifyingEventHex, EXT_WRITE_ROWS, tableMaps);
        assertThat(modifyingEventBody.getNewRows(), equalTo(ImmutableList.of(new Row("Ų"))));
    }

    @Ignore
    @Test
    public void utf8mb4Charset() throws IOException {
        // Query: INSERT INTO capture_binlog_events.foo (utf8mb4Charset) VALUES ('Þ')

        Map<Long, TableMapEventBody> tableMaps = new HashMap<>();

        byte[] tableMapHex =
                parseHexBinary("2D0100000000010015636170747572655F62696E6C6F675F6576656E74730003666F6F0001FE02FE0401");
        bodyParser.parse(tableMapHex, TABLE_MAP, tableMaps);

        byte[] modifyingEventHex =
                parseHexBinary("2D01000000000100020001FFFE02C39E");
        ModifyingEventBody modifyingEventBody = (ModifyingEventBody) bodyParser.parse(modifyingEventHex, EXT_WRITE_ROWS, tableMaps);
        assertThat(modifyingEventBody.getNewRows(), equalTo(ImmutableList.of(new Row("Þ"))));
    }

    @Ignore
    @Test
    public void cp1251Charset() throws IOException {
        // Query: INSERT INTO capture_binlog_events.foo (cp1251Charset) VALUES ('¶')

        Map<Long, TableMapEventBody> tableMaps = new HashMap<>();

        byte[] tableMapHex =
                parseHexBinary("2E0100000000010015636170747572655F62696E6C6F675F6576656E74730003666F6F0001FE02FE0101");
        bodyParser.parse(tableMapHex, TABLE_MAP, tableMaps);

        byte[] modifyingEventHex =
                parseHexBinary("2E01000000000100020001FFFE01B6");
        ModifyingEventBody modifyingEventBody = (ModifyingEventBody) bodyParser.parse(modifyingEventHex, EXT_WRITE_ROWS, tableMaps);
        assertThat(modifyingEventBody.getNewRows(), equalTo(ImmutableList.of(new Row("¶"))));
    }

    @Ignore
    @Test
    public void utf16Charset() throws IOException {
        // Query: INSERT INTO capture_binlog_events.foo (utf16Charset) VALUES ('Ȑ')

        Map<Long, TableMapEventBody> tableMaps = new HashMap<>();

        byte[] tableMapHex =
                parseHexBinary("2F0100000000010015636170747572655F62696E6C6F675F6576656E74730003666F6F0001FE02FE0401");
        bodyParser.parse(tableMapHex, TABLE_MAP, tableMaps);

        byte[] modifyingEventHex =
                parseHexBinary("2F01000000000100020001FFFE020210");
        ModifyingEventBody modifyingEventBody = (ModifyingEventBody) bodyParser.parse(modifyingEventHex, EXT_WRITE_ROWS, tableMaps);
        assertThat(modifyingEventBody.getNewRows(), equalTo(ImmutableList.of(new Row("Ȑ"))));
    }

    @Ignore
    @Test
    public void utf16leCharset() throws IOException {
        // Query: INSERT INTO capture_binlog_events.foo (utf16leCharset) VALUES ('ȸ')

        Map<Long, TableMapEventBody> tableMaps = new HashMap<>();

        byte[] tableMapHex =
                parseHexBinary("300100000000010015636170747572655F62696E6C6F675F6576656E74730003666F6F0001FE02FE0401");
        bodyParser.parse(tableMapHex, TABLE_MAP, tableMaps);

        byte[] modifyingEventHex =
                parseHexBinary("3001000000000100020001FFFE023802");
        ModifyingEventBody modifyingEventBody = (ModifyingEventBody) bodyParser.parse(modifyingEventHex, EXT_WRITE_ROWS, tableMaps);
        assertThat(modifyingEventBody.getNewRows(), equalTo(ImmutableList.of(new Row("ȸ"))));
    }

    @Ignore
    @Test
    public void cp1256Charset() throws IOException {
        // Query: INSERT INTO capture_binlog_events.foo (cp1256Charset) VALUES ('ش')

        Map<Long, TableMapEventBody> tableMaps = new HashMap<>();

        byte[] tableMapHex =
                parseHexBinary("310100000000010015636170747572655F62696E6C6F675F6576656E74730003666F6F0001FE02FE0101");
        bodyParser.parse(tableMapHex, TABLE_MAP, tableMaps);

        byte[] modifyingEventHex =
                parseHexBinary("3101000000000100020001FFFE01D4");
        ModifyingEventBody modifyingEventBody = (ModifyingEventBody) bodyParser.parse(modifyingEventHex, EXT_WRITE_ROWS, tableMaps);
        assertThat(modifyingEventBody.getNewRows(), equalTo(ImmutableList.of(new Row("ش"))));
    }

    @Ignore
    @Test
    public void cp1257Charset() throws IOException {
        // Query: INSERT INTO capture_binlog_events.foo (cp1257Charset) VALUES ('å')

        Map<Long, TableMapEventBody> tableMaps = new HashMap<>();

        byte[] tableMapHex =
                parseHexBinary("320100000000010015636170747572655F62696E6C6F675F6576656E74730003666F6F0001FE02FE0101");
        bodyParser.parse(tableMapHex, TABLE_MAP, tableMaps);

        byte[] modifyingEventHex =
                parseHexBinary("3201000000000100020001FFFE01E5");
        ModifyingEventBody modifyingEventBody = (ModifyingEventBody) bodyParser.parse(modifyingEventHex, EXT_WRITE_ROWS, tableMaps);
        assertThat(modifyingEventBody.getNewRows(), equalTo(ImmutableList.of(new Row("å"))));
    }

    @Ignore
    @Test
    public void utf32Charset() throws IOException {
        // Query: INSERT INTO capture_binlog_events.foo (utf32Charset) VALUES ('ʆ')

        Map<Long, TableMapEventBody> tableMaps = new HashMap<>();

        byte[] tableMapHex =
                parseHexBinary("330100000000010015636170747572655F62696E6C6F675F6576656E74730003666F6F0001FE02FE0401");
        bodyParser.parse(tableMapHex, TABLE_MAP, tableMaps);

        byte[] modifyingEventHex =
                parseHexBinary("3301000000000100020001FFFE0400000286");
        ModifyingEventBody modifyingEventBody = (ModifyingEventBody) bodyParser.parse(modifyingEventHex, EXT_WRITE_ROWS, tableMaps);
        assertThat(modifyingEventBody.getNewRows(), equalTo(ImmutableList.of(new Row("ʆ"))));
    }

    @Ignore
    @Test
    public void cp932Charset() throws IOException {
        // Query: INSERT INTO capture_binlog_events.foo (cp932Charset) VALUES ('ﾂ')

        Map<Long, TableMapEventBody> tableMaps = new HashMap<>();

        byte[] tableMapHex =
                parseHexBinary("340100000000010015636170747572655F62696E6C6F675F6576656E74730003666F6F0001FE02FE0201");
        bodyParser.parse(tableMapHex, TABLE_MAP, tableMaps);

        byte[] modifyingEventHex =
                parseHexBinary("3401000000000100020001FFFE01C2");
        ModifyingEventBody modifyingEventBody = (ModifyingEventBody) bodyParser.parse(modifyingEventHex, EXT_WRITE_ROWS, tableMaps);
        assertThat(modifyingEventBody.getNewRows(), equalTo(ImmutableList.of(new Row("ﾂ"))));
    }

    @Ignore
    @Test
    public void gb18030Charset() throws IOException {
        // Query: INSERT INTO capture_binlog_events.foo (gb18030Charset) VALUES ('Θ')

        Map<Long, TableMapEventBody> tableMaps = new HashMap<>();

        byte[] tableMapHex =
                parseHexBinary("350100000000010015636170747572655F62696E6C6F675F6576656E74730003666F6F0001FE02FE0401");
        bodyParser.parse(tableMapHex, TABLE_MAP, tableMaps);

        byte[] modifyingEventHex =
                parseHexBinary("3501000000000100020001FFFE02A6A8");
        ModifyingEventBody modifyingEventBody = (ModifyingEventBody) bodyParser.parse(modifyingEventHex, EXT_WRITE_ROWS, tableMaps);
        assertThat(modifyingEventBody.getNewRows(), equalTo(ImmutableList.of(new Row("Θ"))));
    }

    @Test
    public void singleChar() throws IOException {
        // Query: INSERT INTO capture_binlog_events.foo (singleChar) VALUES ('a')

        Map<Long, TableMapEventBody> tableMaps = new HashMap<>();

        byte[] tableMapHex =
                parseHexBinary("360100000000010015636170747572655F62696E6C6F675F6576656E74730003666F6F0001FE02FE0301");
        bodyParser.parse(tableMapHex, TABLE_MAP, tableMaps);

        byte[] modifyingEventHex =
                parseHexBinary("3601000000000100020001FFFE0161");
        ModifyingEventBody modifyingEventBody = (ModifyingEventBody) bodyParser.parse(modifyingEventHex, EXT_WRITE_ROWS, tableMaps);
        assertThat(modifyingEventBody.getNewRows(), equalTo(ImmutableList.of(new Row("a"))));
    }

    @Test
    public void multiChar() throws IOException {
        // Query: INSERT INTO capture_binlog_events.foo (multiChar) VALUES ('foobarbazqux')

        Map<Long, TableMapEventBody> tableMaps = new HashMap<>();

        byte[] tableMapHex =
                parseHexBinary("370100000000010015636170747572655F62696E6C6F675F6576656E74730003666F6F0001FE02FE4B01");
        bodyParser.parse(tableMapHex, TABLE_MAP, tableMaps);

        byte[] modifyingEventHex =
                parseHexBinary("3701000000000100020001FFFE0C666F6F62617262617A717578");
        ModifyingEventBody modifyingEventBody = (ModifyingEventBody) bodyParser.parse(modifyingEventHex, EXT_WRITE_ROWS, tableMaps);
        assertThat(modifyingEventBody.getNewRows(), equalTo(ImmutableList.of(new Row("foobarbazqux"))));
    }

    @Test
    public void emptyChar() throws IOException {
        // Query: INSERT INTO capture_binlog_events.foo (emptyChar) VALUES ('')

        Map<Long, TableMapEventBody> tableMaps = new HashMap<>();

        byte[] tableMapHex =
                parseHexBinary("380100000000010015636170747572655F62696E6C6F675F6576656E74730003666F6F0001FE02FE0301");
        bodyParser.parse(tableMapHex, TABLE_MAP, tableMaps);

        byte[] modifyingEventHex =
                parseHexBinary("3801000000000100020001FFFE00");
        ModifyingEventBody modifyingEventBody = (ModifyingEventBody) bodyParser.parse(modifyingEventHex, EXT_WRITE_ROWS, tableMaps);
        assertThat(modifyingEventBody.getNewRows(), equalTo(ImmutableList.of(new Row(""))));
    }

    @Test
    public void nullChar() throws IOException {
        // Query: INSERT INTO capture_binlog_events.foo (nullChar) VALUES (null)

        Map<Long, TableMapEventBody> tableMaps = new HashMap<>();

        byte[] tableMapHex =
                parseHexBinary("390100000000010015636170747572655F62696E6C6F675F6576656E74730003666F6F0001FE02FE0301");
        bodyParser.parse(tableMapHex, TABLE_MAP, tableMaps);

        byte[] modifyingEventHex =
                parseHexBinary("3901000000000100020001FFFF");
        ModifyingEventBody modifyingEventBody = (ModifyingEventBody) bodyParser.parse(modifyingEventHex, EXT_WRITE_ROWS, tableMaps);
        assertThat(modifyingEventBody.getNewRows(), equalTo(ImmutableList.of(new Row() {{
            add(null);
        }})));
    }

    @Test
    public void singleVarchar() throws IOException {
        // Query: INSERT INTO capture_binlog_events.foo (singleVarchar) VALUES ('a')

        Map<Long, TableMapEventBody> tableMaps = new HashMap<>();

        byte[] tableMapHex =
                parseHexBinary("3A0100000000010015636170747572655F62696E6C6F675F6576656E74730003666F6F00010F02030001");
        bodyParser.parse(tableMapHex, TABLE_MAP, tableMaps);

        byte[] modifyingEventHex =
                parseHexBinary("3A01000000000100020001FFFE0161");
        ModifyingEventBody modifyingEventBody = (ModifyingEventBody) bodyParser.parse(modifyingEventHex, EXT_WRITE_ROWS, tableMaps);
        assertThat(modifyingEventBody.getNewRows(), equalTo(ImmutableList.of(new Row("a"))));
    }

    @Test
    public void multiVarchar() throws IOException {
        // Query: INSERT INTO capture_binlog_events.foo (multiVarchar) VALUES ('foobarbazqux')

        Map<Long, TableMapEventBody> tableMaps = new HashMap<>();

        byte[] tableMapHex =
                parseHexBinary("3B0100000000010015636170747572655F62696E6C6F675F6576656E74730003666F6F00010F024B0001");
        bodyParser.parse(tableMapHex, TABLE_MAP, tableMaps);

        byte[] modifyingEventHex =
                parseHexBinary("3B01000000000100020001FFFE0C666F6F62617262617A717578");
        ModifyingEventBody modifyingEventBody = (ModifyingEventBody) bodyParser.parse(modifyingEventHex, EXT_WRITE_ROWS, tableMaps);
        assertThat(modifyingEventBody.getNewRows(), equalTo(ImmutableList.of(new Row("foobarbazqux"))));
    }

    @Test
    public void emptyVarchar() throws IOException {
        // Query: INSERT INTO capture_binlog_events.foo (emptyVarchar) VALUES ('')

        Map<Long, TableMapEventBody> tableMaps = new HashMap<>();

        byte[] tableMapHex =
                parseHexBinary("3C0100000000010015636170747572655F62696E6C6F675F6576656E74730003666F6F00010F02030001");
        bodyParser.parse(tableMapHex, TABLE_MAP, tableMaps);

        byte[] modifyingEventHex =
                parseHexBinary("3C01000000000100020001FFFE00");
        ModifyingEventBody modifyingEventBody = (ModifyingEventBody) bodyParser.parse(modifyingEventHex, EXT_WRITE_ROWS, tableMaps);
        assertThat(modifyingEventBody.getNewRows(), equalTo(ImmutableList.of(new Row(""))));
    }

    @Test
    public void nullVarchar() throws IOException {
        // Query: INSERT INTO capture_binlog_events.foo (nullVarchar) VALUES (null)

        Map<Long, TableMapEventBody> tableMaps = new HashMap<>();

        byte[] tableMapHex =
                parseHexBinary("3D0100000000010015636170747572655F62696E6C6F675F6576656E74730003666F6F00010F02030001");
        bodyParser.parse(tableMapHex, TABLE_MAP, tableMaps);

        byte[] modifyingEventHex =
                parseHexBinary("3D01000000000100020001FFFF");
        ModifyingEventBody modifyingEventBody = (ModifyingEventBody) bodyParser.parse(modifyingEventHex, EXT_WRITE_ROWS, tableMaps);
        assertThat(modifyingEventBody.getNewRows(), equalTo(ImmutableList.of(new Row() {{
            add(null);
        }})));
    }

    @Test
    public void singleTinytext() throws IOException {
        // Query: INSERT INTO capture_binlog_events.foo (singleTinytext) VALUES ('a')

        Map<Long, TableMapEventBody> tableMaps = new HashMap<>();

        byte[] tableMapHex =
                parseHexBinary("3E0100000000010015636170747572655F62696E6C6F675F6576656E74730003666F6F0001FC010101");
        bodyParser.parse(tableMapHex, TABLE_MAP, tableMaps);

        byte[] modifyingEventHex =
                parseHexBinary("3E01000000000100020001FFFE0161");
        ModifyingEventBody modifyingEventBody = (ModifyingEventBody) bodyParser.parse(modifyingEventHex, EXT_WRITE_ROWS, tableMaps);
        assertThat(modifyingEventBody.getNewRows(), equalTo(ImmutableList.of(new Row("a"))));
    }

    @Test
    public void multiTinytext() throws IOException {
        // Query: INSERT INTO capture_binlog_events.foo (multiTinytext) VALUES ('foobarbazqux')

        Map<Long, TableMapEventBody> tableMaps = new HashMap<>();

        byte[] tableMapHex =
                parseHexBinary("3F0100000000010015636170747572655F62696E6C6F675F6576656E74730003666F6F0001FC010101");
        bodyParser.parse(tableMapHex, TABLE_MAP, tableMaps);

        byte[] modifyingEventHex =
                parseHexBinary("3F01000000000100020001FFFE0C666F6F62617262617A717578");
        ModifyingEventBody modifyingEventBody = (ModifyingEventBody) bodyParser.parse(modifyingEventHex, EXT_WRITE_ROWS, tableMaps);
        assertThat(modifyingEventBody.getNewRows(), equalTo(ImmutableList.of(new Row("foobarbazqux"))));
    }

    @Test
    public void emptyTinytext() throws IOException {
        // Query: INSERT INTO capture_binlog_events.foo (emptyTinytext) VALUES ('')

        Map<Long, TableMapEventBody> tableMaps = new HashMap<>();

        byte[] tableMapHex =
                parseHexBinary("400100000000010015636170747572655F62696E6C6F675F6576656E74730003666F6F0001FC010101");
        bodyParser.parse(tableMapHex, TABLE_MAP, tableMaps);

        byte[] modifyingEventHex =
                parseHexBinary("4001000000000100020001FFFE00");
        ModifyingEventBody modifyingEventBody = (ModifyingEventBody) bodyParser.parse(modifyingEventHex, EXT_WRITE_ROWS, tableMaps);
        assertThat(modifyingEventBody.getNewRows(), equalTo(ImmutableList.of(new Row(""))));
    }

    @Test
    public void nullTinytext() throws IOException {
        // Query: INSERT INTO capture_binlog_events.foo (nullTinytext) VALUES (null)

        Map<Long, TableMapEventBody> tableMaps = new HashMap<>();

        byte[] tableMapHex =
                parseHexBinary("410100000000010015636170747572655F62696E6C6F675F6576656E74730003666F6F0001FC010101");
        bodyParser.parse(tableMapHex, TABLE_MAP, tableMaps);

        byte[] modifyingEventHex =
                parseHexBinary("4101000000000100020001FFFF");
        ModifyingEventBody modifyingEventBody = (ModifyingEventBody) bodyParser.parse(modifyingEventHex, EXT_WRITE_ROWS, tableMaps);
        assertThat(modifyingEventBody.getNewRows(), equalTo(ImmutableList.of(new Row() {{
            add(null);
        }})));
    }

    @Test
    public void singleMediumtext() throws IOException {
        // Query: INSERT INTO capture_binlog_events.foo (singleMediumtext) VALUES ('a')

        Map<Long, TableMapEventBody> tableMaps = new HashMap<>();

        byte[] tableMapHex =
                parseHexBinary("420100000000010015636170747572655F62696E6C6F675F6576656E74730003666F6F0001FC010301");
        bodyParser.parse(tableMapHex, TABLE_MAP, tableMaps);

        byte[] modifyingEventHex =
                parseHexBinary("4201000000000100020001FFFE01000061");
        ModifyingEventBody modifyingEventBody = (ModifyingEventBody) bodyParser.parse(modifyingEventHex, EXT_WRITE_ROWS, tableMaps);
        assertThat(modifyingEventBody.getNewRows(), equalTo(ImmutableList.of(new Row("a"))));
    }

    @Test
    public void multiMediumtext() throws IOException {
        // Query: INSERT INTO capture_binlog_events.foo (multiMediumtext) VALUES ('foobarbazqux')

        Map<Long, TableMapEventBody> tableMaps = new HashMap<>();

        byte[] tableMapHex =
                parseHexBinary("430100000000010015636170747572655F62696E6C6F675F6576656E74730003666F6F0001FC010301");
        bodyParser.parse(tableMapHex, TABLE_MAP, tableMaps);

        byte[] modifyingEventHex =
                parseHexBinary("4301000000000100020001FFFE0C0000666F6F62617262617A717578");
        ModifyingEventBody modifyingEventBody = (ModifyingEventBody) bodyParser.parse(modifyingEventHex, EXT_WRITE_ROWS, tableMaps);
        assertThat(modifyingEventBody.getNewRows(), equalTo(ImmutableList.of(new Row("foobarbazqux"))));
    }

    @Test
    public void emptyMediumtext() throws IOException {
        // Query: INSERT INTO capture_binlog_events.foo (emptyMediumtext) VALUES ('')

        Map<Long, TableMapEventBody> tableMaps = new HashMap<>();

        byte[] tableMapHex =
                parseHexBinary("440100000000010015636170747572655F62696E6C6F675F6576656E74730003666F6F0001FC010301");
        bodyParser.parse(tableMapHex, TABLE_MAP, tableMaps);

        byte[] modifyingEventHex =
                parseHexBinary("4401000000000100020001FFFE000000");
        ModifyingEventBody modifyingEventBody = (ModifyingEventBody) bodyParser.parse(modifyingEventHex, EXT_WRITE_ROWS, tableMaps);
        assertThat(modifyingEventBody.getNewRows(), equalTo(ImmutableList.of(new Row(""))));
    }

    @Test
    public void nullMediumtext() throws IOException {
        // Query: INSERT INTO capture_binlog_events.foo (nullMediumtext) VALUES (null)

        Map<Long, TableMapEventBody> tableMaps = new HashMap<>();

        byte[] tableMapHex =
                parseHexBinary("450100000000010015636170747572655F62696E6C6F675F6576656E74730003666F6F0001FC010301");
        bodyParser.parse(tableMapHex, TABLE_MAP, tableMaps);

        byte[] modifyingEventHex =
                parseHexBinary("4501000000000100020001FFFF");
        ModifyingEventBody modifyingEventBody = (ModifyingEventBody) bodyParser.parse(modifyingEventHex, EXT_WRITE_ROWS, tableMaps);
        assertThat(modifyingEventBody.getNewRows(), equalTo(ImmutableList.of(new Row() {{
            add(null);
        }})));
    }

    @Test
    public void singleLongtext() throws IOException {
        // Query: INSERT INTO capture_binlog_events.foo (singleLongtext) VALUES ('a')

        Map<Long, TableMapEventBody> tableMaps = new HashMap<>();

        byte[] tableMapHex =
                parseHexBinary("460100000000010015636170747572655F62696E6C6F675F6576656E74730003666F6F0001FC010401");
        bodyParser.parse(tableMapHex, TABLE_MAP, tableMaps);

        byte[] modifyingEventHex =
                parseHexBinary("4601000000000100020001FFFE0100000061");
        ModifyingEventBody modifyingEventBody = (ModifyingEventBody) bodyParser.parse(modifyingEventHex, EXT_WRITE_ROWS, tableMaps);
        assertThat(modifyingEventBody.getNewRows(), equalTo(ImmutableList.of(new Row("a"))));
    }

    @Test
    public void multiLongtext() throws IOException {
        // Query: INSERT INTO capture_binlog_events.foo (multiLongtext) VALUES ('foobarbazqux')

        Map<Long, TableMapEventBody> tableMaps = new HashMap<>();

        byte[] tableMapHex =
                parseHexBinary("470100000000010015636170747572655F62696E6C6F675F6576656E74730003666F6F0001FC010401");
        bodyParser.parse(tableMapHex, TABLE_MAP, tableMaps);

        byte[] modifyingEventHex =
                parseHexBinary("4701000000000100020001FFFE0C000000666F6F62617262617A717578");
        ModifyingEventBody modifyingEventBody = (ModifyingEventBody) bodyParser.parse(modifyingEventHex, EXT_WRITE_ROWS, tableMaps);
        assertThat(modifyingEventBody.getNewRows(), equalTo(ImmutableList.of(new Row("foobarbazqux"))));
    }

    @Test
    public void emptyLongtext() throws IOException {
        // Query: INSERT INTO capture_binlog_events.foo (emptyLongtext) VALUES ('')

        Map<Long, TableMapEventBody> tableMaps = new HashMap<>();

        byte[] tableMapHex =
                parseHexBinary("480100000000010015636170747572655F62696E6C6F675F6576656E74730003666F6F0001FC010401");
        bodyParser.parse(tableMapHex, TABLE_MAP, tableMaps);

        byte[] modifyingEventHex =
                parseHexBinary("4801000000000100020001FFFE00000000");
        ModifyingEventBody modifyingEventBody = (ModifyingEventBody) bodyParser.parse(modifyingEventHex, EXT_WRITE_ROWS, tableMaps);
        assertThat(modifyingEventBody.getNewRows(), equalTo(ImmutableList.of(new Row(""))));
    }

    @Test
    public void nullLongtext() throws IOException {
        // Query: INSERT INTO capture_binlog_events.foo (nullLongtext) VALUES (null)

        Map<Long, TableMapEventBody> tableMaps = new HashMap<>();

        byte[] tableMapHex =
                parseHexBinary("490100000000010015636170747572655F62696E6C6F675F6576656E74730003666F6F0001FC010401");
        bodyParser.parse(tableMapHex, TABLE_MAP, tableMaps);

        byte[] modifyingEventHex =
                parseHexBinary("4901000000000100020001FFFF");
        ModifyingEventBody modifyingEventBody = (ModifyingEventBody) bodyParser.parse(modifyingEventHex, EXT_WRITE_ROWS, tableMaps);
        assertThat(modifyingEventBody.getNewRows(), equalTo(ImmutableList.of(new Row() {{
            add(null);
        }})));
    }

    @Ignore
    @Test
    public void singleEnum() throws IOException {
        // Query: INSERT INTO capture_binlog_events.foo (singleEnum) VALUES ('a')

        Map<Long, TableMapEventBody> tableMaps = new HashMap<>();

        byte[] tableMapHex =
                parseHexBinary("4A0100000000010015636170747572655F62696E6C6F675F6576656E74730003666F6F0001FE02F70101");
        bodyParser.parse(tableMapHex, TABLE_MAP, tableMaps);

        byte[] modifyingEventHex =
                parseHexBinary("4A01000000000100020001FFFE01");
        ModifyingEventBody modifyingEventBody = (ModifyingEventBody) bodyParser.parse(modifyingEventHex, EXT_WRITE_ROWS, tableMaps);
        assertThat(modifyingEventBody.getNewRows(), equalTo(ImmutableList.of(new Row("a"))));
    }

    @Ignore
    @Test
    public void multiEmum() throws IOException {
        // Query: INSERT INTO capture_binlog_events.foo (multiEmum) VALUES ('foobar')

        Map<Long, TableMapEventBody> tableMaps = new HashMap<>();

        byte[] tableMapHex =
                parseHexBinary("4B0100000000010015636170747572655F62696E6C6F675F6576656E74730003666F6F0001FE02F70101");
        bodyParser.parse(tableMapHex, TABLE_MAP, tableMaps);

        byte[] modifyingEventHex =
                parseHexBinary("4B01000000000100020001FFFE01");
        ModifyingEventBody modifyingEventBody = (ModifyingEventBody) bodyParser.parse(modifyingEventHex, EXT_WRITE_ROWS, tableMaps);
        assertThat(modifyingEventBody.getNewRows(), equalTo(ImmutableList.of(new Row("foobar"))));
    }

    @Ignore
    @Test
    public void emptyEmum() throws IOException {
        // Query: INSERT INTO capture_binlog_events.foo (emptyEmum) VALUES ('')

        Map<Long, TableMapEventBody> tableMaps = new HashMap<>();

        byte[] tableMapHex =
                parseHexBinary("4C0100000000010015636170747572655F62696E6C6F675F6576656E74730003666F6F0001FE02F70101");
        bodyParser.parse(tableMapHex, TABLE_MAP, tableMaps);

        byte[] modifyingEventHex =
                parseHexBinary("4C01000000000100020001FFFE01");
        ModifyingEventBody modifyingEventBody = (ModifyingEventBody) bodyParser.parse(modifyingEventHex, EXT_WRITE_ROWS, tableMaps);
        assertThat(modifyingEventBody.getNewRows(), equalTo(ImmutableList.of(new Row(""))));
    }

    @Test
    public void nullEnum() throws IOException {
        // Query: INSERT INTO capture_binlog_events.foo (nullEnum) VALUES (null)

        Map<Long, TableMapEventBody> tableMaps = new HashMap<>();

        byte[] tableMapHex =
                parseHexBinary("4D0100000000010015636170747572655F62696E6C6F675F6576656E74730003666F6F0001FE02F70101");
        bodyParser.parse(tableMapHex, TABLE_MAP, tableMaps);

        byte[] modifyingEventHex =
                parseHexBinary("4D01000000000100020001FFFF");
        ModifyingEventBody modifyingEventBody = (ModifyingEventBody) bodyParser.parse(modifyingEventHex, EXT_WRITE_ROWS, tableMaps);
        assertThat(modifyingEventBody.getNewRows(), equalTo(ImmutableList.of(new Row() {{
            add(null);
        }})));
    }

    @Ignore
    @Test
    public void singleSet() throws IOException {
        // Query: INSERT INTO capture_binlog_events.foo (singleSet) VALUES ('a')

        Map<Long, TableMapEventBody> tableMaps = new HashMap<>();

        byte[] tableMapHex =
                parseHexBinary("4E0100000000010015636170747572655F62696E6C6F675F6576656E74730003666F6F0001FE02F80101");
        bodyParser.parse(tableMapHex, TABLE_MAP, tableMaps);

        byte[] modifyingEventHex =
                parseHexBinary("4E01000000000100020001FFFE01");
        ModifyingEventBody modifyingEventBody = (ModifyingEventBody) bodyParser.parse(modifyingEventHex, EXT_WRITE_ROWS, tableMaps);
        assertThat(modifyingEventBody.getNewRows(), equalTo(ImmutableList.of(new Row("a"))));
    }

    @Ignore
    @Test
    public void multiSet() throws IOException {
        // Query: INSERT INTO capture_binlog_events.foo (multiSet) VALUES ('foobar')

        Map<Long, TableMapEventBody> tableMaps = new HashMap<>();

        byte[] tableMapHex =
                parseHexBinary("4F0100000000010015636170747572655F62696E6C6F675F6576656E74730003666F6F0001FE02F80101");
        bodyParser.parse(tableMapHex, TABLE_MAP, tableMaps);

        byte[] modifyingEventHex =
                parseHexBinary("4F01000000000100020001FFFE01");
        ModifyingEventBody modifyingEventBody = (ModifyingEventBody) bodyParser.parse(modifyingEventHex, EXT_WRITE_ROWS, tableMaps);
        assertThat(modifyingEventBody.getNewRows(), equalTo(ImmutableList.of(new Row("foobar"))));
    }

    @Test
    public void emptySet() throws IOException {
        // Query: INSERT INTO capture_binlog_events.foo (emptySet) VALUES ('')

        Map<Long, TableMapEventBody> tableMaps = new HashMap<>();

        byte[] tableMapHex =
                parseHexBinary("500100000000010015636170747572655F62696E6C6F675F6576656E74730003666F6F0001FE02F80101");
        bodyParser.parse(tableMapHex, TABLE_MAP, tableMaps);

        byte[] modifyingEventHex =
                parseHexBinary("5001000000000100020001FFFE00");
        ModifyingEventBody modifyingEventBody = (ModifyingEventBody) bodyParser.parse(modifyingEventHex, EXT_WRITE_ROWS, tableMaps);
        assertThat(modifyingEventBody.getNewRows(), equalTo(ImmutableList.of(new Row("0"))));
    }

    @Test
    public void nullSet() throws IOException {
        // Query: INSERT INTO capture_binlog_events.foo (nullSet) VALUES (null)

        Map<Long, TableMapEventBody> tableMaps = new HashMap<>();

        byte[] tableMapHex =
                parseHexBinary("510100000000010015636170747572655F62696E6C6F675F6576656E74730003666F6F0001FE02F80101");
        bodyParser.parse(tableMapHex, TABLE_MAP, tableMaps);

        byte[] modifyingEventHex =
                parseHexBinary("5101000000000100020001FFFF");
        ModifyingEventBody modifyingEventBody = (ModifyingEventBody) bodyParser.parse(modifyingEventHex, EXT_WRITE_ROWS, tableMaps);
        assertThat(modifyingEventBody.getNewRows(), equalTo(ImmutableList.of(new Row() {{
            add(null);
        }})));
    }

    @Test
    public void minTinyint() throws IOException {
        // Query: INSERT INTO capture_binlog_events.foo (minTinyint) VALUES (-128)

        Map<Long, TableMapEventBody> tableMaps = new HashMap<>();

        byte[] tableMapHex =
                parseHexBinary("520100000000010015636170747572655F62696E6C6F675F6576656E74730003666F6F0001010001");
        bodyParser.parse(tableMapHex, TABLE_MAP, tableMaps);

        byte[] modifyingEventHex =
                parseHexBinary("5201000000000100020001FFFE80");
        ModifyingEventBody modifyingEventBody = (ModifyingEventBody) bodyParser.parse(modifyingEventHex, EXT_WRITE_ROWS, tableMaps);
        assertThat(modifyingEventBody.getNewRows(), equalTo(ImmutableList.of(new Row("-128"))));
    }

    @Test
    public void maxTinyint() throws IOException {
        // Query: INSERT INTO capture_binlog_events.foo (maxTinyint) VALUES (127)

        Map<Long, TableMapEventBody> tableMaps = new HashMap<>();

        byte[] tableMapHex =
                parseHexBinary("530100000000010015636170747572655F62696E6C6F675F6576656E74730003666F6F0001010001");
        bodyParser.parse(tableMapHex, TABLE_MAP, tableMaps);

        byte[] modifyingEventHex =
                parseHexBinary("5301000000000100020001FFFE7F");
        ModifyingEventBody modifyingEventBody = (ModifyingEventBody) bodyParser.parse(modifyingEventHex, EXT_WRITE_ROWS, tableMaps);
        assertThat(modifyingEventBody.getNewRows(), equalTo(ImmutableList.of(new Row("127"))));
    }

    @Test
    public void nullTinyint() throws IOException {
        // Query: INSERT INTO capture_binlog_events.foo (nullTinyint) VALUES (null)

        Map<Long, TableMapEventBody> tableMaps = new HashMap<>();

        byte[] tableMapHex =
                parseHexBinary("540100000000010015636170747572655F62696E6C6F675F6576656E74730003666F6F0001010001");
        bodyParser.parse(tableMapHex, TABLE_MAP, tableMaps);

        byte[] modifyingEventHex =
                parseHexBinary("5401000000000100020001FFFF");
        ModifyingEventBody modifyingEventBody = (ModifyingEventBody) bodyParser.parse(modifyingEventHex, EXT_WRITE_ROWS, tableMaps);
        assertThat(modifyingEventBody.getNewRows(), equalTo(ImmutableList.of(new Row() {{
            add(null);
        }})));
    }

    @Test
    public void minSmallint() throws IOException {
        // Query: INSERT INTO capture_binlog_events.foo (minSmallint) VALUES (32767)

        Map<Long, TableMapEventBody> tableMaps = new HashMap<>();

        byte[] tableMapHex =
                parseHexBinary("550100000000010015636170747572655F62696E6C6F675F6576656E74730003666F6F0001020001");
        bodyParser.parse(tableMapHex, TABLE_MAP, tableMaps);

        byte[] modifyingEventHex =
                parseHexBinary("5501000000000100020001FFFEFF7F");
        ModifyingEventBody modifyingEventBody = (ModifyingEventBody) bodyParser.parse(modifyingEventHex, EXT_WRITE_ROWS, tableMaps);
        assertThat(modifyingEventBody.getNewRows(), equalTo(ImmutableList.of(new Row("32767"))));
    }

    @Test
    public void maxSmallint() throws IOException {
        // Query: INSERT INTO capture_binlog_events.foo (maxSmallint) VALUES (-32768)

        Map<Long, TableMapEventBody> tableMaps = new HashMap<>();

        byte[] tableMapHex =
                parseHexBinary("560100000000010015636170747572655F62696E6C6F675F6576656E74730003666F6F0001020001");
        bodyParser.parse(tableMapHex, TABLE_MAP, tableMaps);

        byte[] modifyingEventHex =
                parseHexBinary("5601000000000100020001FFFE0080");
        ModifyingEventBody modifyingEventBody = (ModifyingEventBody) bodyParser.parse(modifyingEventHex, EXT_WRITE_ROWS, tableMaps);
        assertThat(modifyingEventBody.getNewRows(), equalTo(ImmutableList.of(new Row("-32768"))));
    }

    @Test
    public void nullSmallint() throws IOException {
        // Query: INSERT INTO capture_binlog_events.foo (nullSmallint) VALUES (null)

        Map<Long, TableMapEventBody> tableMaps = new HashMap<>();

        byte[] tableMapHex =
                parseHexBinary("570100000000010015636170747572655F62696E6C6F675F6576656E74730003666F6F0001020001");
        bodyParser.parse(tableMapHex, TABLE_MAP, tableMaps);

        byte[] modifyingEventHex =
                parseHexBinary("5701000000000100020001FFFF");
        ModifyingEventBody modifyingEventBody = (ModifyingEventBody) bodyParser.parse(modifyingEventHex, EXT_WRITE_ROWS, tableMaps);
        assertThat(modifyingEventBody.getNewRows(), equalTo(ImmutableList.of(new Row() {{
            add(null);
        }})));
    }

    @Test
    public void minMediumint() throws IOException {
        // Query: INSERT INTO capture_binlog_events.foo (minMediumint) VALUES (-8388608)

        Map<Long, TableMapEventBody> tableMaps = new HashMap<>();

        byte[] tableMapHex =
                parseHexBinary("580100000000010015636170747572655F62696E6C6F675F6576656E74730003666F6F0001090001");
        bodyParser.parse(tableMapHex, TABLE_MAP, tableMaps);

        byte[] modifyingEventHex =
                parseHexBinary("5801000000000100020001FFFE000080");
        ModifyingEventBody modifyingEventBody = (ModifyingEventBody) bodyParser.parse(modifyingEventHex, EXT_WRITE_ROWS, tableMaps);
        assertThat(modifyingEventBody.getNewRows(), equalTo(ImmutableList.of(new Row("-8388608"))));
    }

    @Test
    public void maxMediumint() throws IOException {
        // Query: INSERT INTO capture_binlog_events.foo (maxMediumint) VALUES (8388607)

        Map<Long, TableMapEventBody> tableMaps = new HashMap<>();

        byte[] tableMapHex =
                parseHexBinary("590100000000010015636170747572655F62696E6C6F675F6576656E74730003666F6F0001090001");
        bodyParser.parse(tableMapHex, TABLE_MAP, tableMaps);

        byte[] modifyingEventHex =
                parseHexBinary("5901000000000100020001FFFEFFFF7F");
        ModifyingEventBody modifyingEventBody = (ModifyingEventBody) bodyParser.parse(modifyingEventHex, EXT_WRITE_ROWS, tableMaps);
        assertThat(modifyingEventBody.getNewRows(), equalTo(ImmutableList.of(new Row("8388607"))));
    }

    @Test
    public void nullMediumint() throws IOException {
        // Query: INSERT INTO capture_binlog_events.foo (nullMediumint) VALUES (null)

        Map<Long, TableMapEventBody> tableMaps = new HashMap<>();

        byte[] tableMapHex =
                parseHexBinary("5A0100000000010015636170747572655F62696E6C6F675F6576656E74730003666F6F0001090001");
        bodyParser.parse(tableMapHex, TABLE_MAP, tableMaps);

        byte[] modifyingEventHex =
                parseHexBinary("5A01000000000100020001FFFF");
        ModifyingEventBody modifyingEventBody = (ModifyingEventBody) bodyParser.parse(modifyingEventHex, EXT_WRITE_ROWS, tableMaps);
        assertThat(modifyingEventBody.getNewRows(), equalTo(ImmutableList.of(new Row() {{
            add(null);
        }})));
    }

    @Test
    public void minInt() throws IOException {
        // Query: INSERT INTO capture_binlog_events.foo (minInt) VALUES (-2147483648)

        Map<Long, TableMapEventBody> tableMaps = new HashMap<>();

        byte[] tableMapHex =
                parseHexBinary("5B0100000000010015636170747572655F62696E6C6F675F6576656E74730003666F6F0001030001");
        bodyParser.parse(tableMapHex, TABLE_MAP, tableMaps);

        byte[] modifyingEventHex =
                parseHexBinary("5B01000000000100020001FFFE00000080");
        ModifyingEventBody modifyingEventBody = (ModifyingEventBody) bodyParser.parse(modifyingEventHex, EXT_WRITE_ROWS, tableMaps);
        assertThat(modifyingEventBody.getNewRows(), equalTo(ImmutableList.of(new Row("-2147483648"))));
    }

    @Test
    public void maxInt() throws IOException {
        // Query: INSERT INTO capture_binlog_events.foo (maxInt) VALUES (2147483647)

        Map<Long, TableMapEventBody> tableMaps = new HashMap<>();

        byte[] tableMapHex =
                parseHexBinary("5C0100000000010015636170747572655F62696E6C6F675F6576656E74730003666F6F0001030001");
        bodyParser.parse(tableMapHex, TABLE_MAP, tableMaps);

        byte[] modifyingEventHex =
                parseHexBinary("5C01000000000100020001FFFEFFFFFF7F");
        ModifyingEventBody modifyingEventBody = (ModifyingEventBody) bodyParser.parse(modifyingEventHex, EXT_WRITE_ROWS, tableMaps);
        assertThat(modifyingEventBody.getNewRows(), equalTo(ImmutableList.of(new Row("2147483647"))));
    }

    @Test
    public void nullInt() throws IOException {
        // Query: INSERT INTO capture_binlog_events.foo (nullInt) VALUES (null)

        Map<Long, TableMapEventBody> tableMaps = new HashMap<>();

        byte[] tableMapHex =
                parseHexBinary("5D0100000000010015636170747572655F62696E6C6F675F6576656E74730003666F6F0001030001");
        bodyParser.parse(tableMapHex, TABLE_MAP, tableMaps);

        byte[] modifyingEventHex =
                parseHexBinary("5D01000000000100020001FFFF");
        ModifyingEventBody modifyingEventBody = (ModifyingEventBody) bodyParser.parse(modifyingEventHex, EXT_WRITE_ROWS, tableMaps);
        assertThat(modifyingEventBody.getNewRows(), equalTo(ImmutableList.of(new Row() {{
            add(null);
        }})));
    }

    @Test
    public void minBigint() throws IOException {
        // Query: INSERT INTO capture_binlog_events.foo (minBigint) VALUES (-9223372036854775808)

        Map<Long, TableMapEventBody> tableMaps = new HashMap<>();

        byte[] tableMapHex =
                parseHexBinary("5E0100000000010015636170747572655F62696E6C6F675F6576656E74730003666F6F0001080001");
        bodyParser.parse(tableMapHex, TABLE_MAP, tableMaps);

        byte[] modifyingEventHex =
                parseHexBinary("5E01000000000100020001FFFE0000000000000080");
        ModifyingEventBody modifyingEventBody = (ModifyingEventBody) bodyParser.parse(modifyingEventHex, EXT_WRITE_ROWS, tableMaps);
        assertThat(modifyingEventBody.getNewRows(), equalTo(ImmutableList.of(new Row("-9223372036854775808"))));
    }

    @Test
    public void maxBigint() throws IOException {
        // Query: INSERT INTO capture_binlog_events.foo (maxBigint) VALUES (9223372036854775807)

        Map<Long, TableMapEventBody> tableMaps = new HashMap<>();

        byte[] tableMapHex =
                parseHexBinary("5F0100000000010015636170747572655F62696E6C6F675F6576656E74730003666F6F0001080001");
        bodyParser.parse(tableMapHex, TABLE_MAP, tableMaps);

        byte[] modifyingEventHex =
                parseHexBinary("5F01000000000100020001FFFEFFFFFFFFFFFFFF7F");
        ModifyingEventBody modifyingEventBody = (ModifyingEventBody) bodyParser.parse(modifyingEventHex, EXT_WRITE_ROWS, tableMaps);
        assertThat(modifyingEventBody.getNewRows(), equalTo(ImmutableList.of(new Row("9223372036854775807"))));
    }

    @Test
    public void nullBigint() throws IOException {
        // Query: INSERT INTO capture_binlog_events.foo (nullBigint) VALUES (null)

        Map<Long, TableMapEventBody> tableMaps = new HashMap<>();

        byte[] tableMapHex =
                parseHexBinary("600100000000010015636170747572655F62696E6C6F675F6576656E74730003666F6F0001080001");
        bodyParser.parse(tableMapHex, TABLE_MAP, tableMaps);

        byte[] modifyingEventHex =
                parseHexBinary("6001000000000100020001FFFF");
        ModifyingEventBody modifyingEventBody = (ModifyingEventBody) bodyParser.parse(modifyingEventHex, EXT_WRITE_ROWS, tableMaps);
        assertThat(modifyingEventBody.getNewRows(), equalTo(ImmutableList.of(new Row() {{
            add(null);
        }})));
    }

    @Test
    public void minDecimal() throws IOException {
        // Query: INSERT INTO capture_binlog_events.foo (minDecimal) VALUES (-99999999999999999999999999999999999.999...')

        Map<Long, TableMapEventBody> tableMaps = new HashMap<>();

        byte[] tableMapHex =
                parseHexBinary("610100000000010015636170747572655F62696E6C6F675F6576656E74730003666F6F0001F602411E01");
        bodyParser.parse(tableMapHex, TABLE_MAP, tableMaps);

        byte[] modifyingEventHex =
                parseHexBinary("6101000000000100020001FFFE7A0A1F00C4653600C4653600C4653600C4653600C4653600C4653600FC18");
        ModifyingEventBody modifyingEventBody = (ModifyingEventBody) bodyParser.parse(modifyingEventHex, EXT_WRITE_ROWS, tableMaps);
        assertThat(modifyingEventBody.getNewRows(), equalTo(ImmutableList.of(new Row("-99999999999999999999999999999999999.999999999999999999999999999999"))));
    }

    @Test
    public void maxDecimal() throws IOException {
        // Query: INSERT INTO capture_binlog_events.foo (maxDecimal) VALUES (99999999999999999999999999999999999.9999...')

        Map<Long, TableMapEventBody> tableMaps = new HashMap<>();

        byte[] tableMapHex =
                parseHexBinary("620100000000010015636170747572655F62696E6C6F675F6576656E74730003666F6F0001F602411E01");
        bodyParser.parse(tableMapHex, TABLE_MAP, tableMaps);

        byte[] modifyingEventHex =
                parseHexBinary("6201000000000100020001FFFE85F5E0FF3B9AC9FF3B9AC9FF3B9AC9FF3B9AC9FF3B9AC9FF3B9AC9FF03E7");
        ModifyingEventBody modifyingEventBody = (ModifyingEventBody) bodyParser.parse(modifyingEventHex, EXT_WRITE_ROWS, tableMaps);
        assertThat(modifyingEventBody.getNewRows(), equalTo(ImmutableList.of(new Row("99999999999999999999999999999999999.999999999999999999999999999999"))));
    }

    @Test
    public void nullDecimal() throws IOException {
        // Query: INSERT INTO capture_binlog_events.foo (nullDecimal) VALUES (null)

        Map<Long, TableMapEventBody> tableMaps = new HashMap<>();

        byte[] tableMapHex =
                parseHexBinary("630100000000010015636170747572655F62696E6C6F675F6576656E74730003666F6F0001F6020A0001");
        bodyParser.parse(tableMapHex, TABLE_MAP, tableMaps);

        byte[] modifyingEventHex =
                parseHexBinary("6301000000000100020001FFFF");
        ModifyingEventBody modifyingEventBody = (ModifyingEventBody) bodyParser.parse(modifyingEventHex, EXT_WRITE_ROWS, tableMaps);
        ArrayList<String> arrayList = new ArrayList<>();
        arrayList.add(null);
        assertThat(modifyingEventBody.getNewRows(), equalTo(ImmutableList.of(new Row() {{
            add(null);
        }})));
    }

    @Test
    public void minFloat() throws IOException {
        // Query: INSERT INTO capture_binlog_events.foo (minFloat) VALUES (-99999999999999999999999999999999999999)

        Map<Long, TableMapEventBody> tableMaps = new HashMap<>();

        byte[] tableMapHex =
                parseHexBinary("640100000000010015636170747572655F62696E6C6F675F6576656E74730003666F6F000104010401");
        bodyParser.parse(tableMapHex, TABLE_MAP, tableMaps);

        byte[] modifyingEventHex =
                parseHexBinary("6401000000000100020001FFFE997696FE");
        ModifyingEventBody modifyingEventBody = (ModifyingEventBody) bodyParser.parse(modifyingEventHex, EXT_WRITE_ROWS, tableMaps);
        assertThat(modifyingEventBody.getNewRows(), equalTo(ImmutableList.of(new Row("-1.0E38"))));
    }

    @Test
    public void maxFloat() throws IOException {
        // Query: INSERT INTO capture_binlog_events.foo (maxFloat) VALUES (99999999999999999999999999999999999999)

        Map<Long, TableMapEventBody> tableMaps = new HashMap<>();

        byte[] tableMapHex =
                parseHexBinary("650100000000010015636170747572655F62696E6C6F675F6576656E74730003666F6F000104010401");
        bodyParser.parse(tableMapHex, TABLE_MAP, tableMaps);

        byte[] modifyingEventHex =
                parseHexBinary("6501000000000100020001FFFE9976967E");
        ModifyingEventBody modifyingEventBody = (ModifyingEventBody) bodyParser.parse(modifyingEventHex, EXT_WRITE_ROWS, tableMaps);
        assertThat(modifyingEventBody.getNewRows(), equalTo(ImmutableList.of(new Row("1.0E38"))));
    }

    @Test
    public void nullFloat() throws IOException {
        // Query: INSERT INTO capture_binlog_events.foo (nullFloat) VALUES (null)

        Map<Long, TableMapEventBody> tableMaps = new HashMap<>();

        byte[] tableMapHex =
                parseHexBinary("660100000000010015636170747572655F62696E6C6F675F6576656E74730003666F6F000104010401");
        bodyParser.parse(tableMapHex, TABLE_MAP, tableMaps);

        byte[] modifyingEventHex =
                parseHexBinary("6601000000000100020001FFFF");
        ModifyingEventBody modifyingEventBody = (ModifyingEventBody) bodyParser.parse(modifyingEventHex, EXT_WRITE_ROWS, tableMaps);
        assertThat(modifyingEventBody.getNewRows(), equalTo(ImmutableList.of(new Row() {{
            add(null);
        }})));
    }

    @Test
    public void minDouble() throws IOException {
        // Query: INSERT INTO capture_binlog_events.foo (minDouble) VALUES (-1.7976931348623157E+308)

        Map<Long, TableMapEventBody> tableMaps = new HashMap<>();

        byte[] tableMapHex =
                parseHexBinary("670100000000010015636170747572655F62696E6C6F675F6576656E74730003666F6F000105010801");
        bodyParser.parse(tableMapHex, TABLE_MAP, tableMaps);

        byte[] modifyingEventHex =
                parseHexBinary("6701000000000100020001FFFEFFFFFFFFFFFFEFFF");
        ModifyingEventBody modifyingEventBody = (ModifyingEventBody) bodyParser.parse(modifyingEventHex, EXT_WRITE_ROWS, tableMaps);
        assertThat(modifyingEventBody.getNewRows(), equalTo(ImmutableList.of(new Row("-1.7976931348623157E308"))));
    }

    @Test
    public void maxDouble() throws IOException {
        // Query: INSERT INTO capture_binlog_events.foo (maxDouble) VALUES (1.7976931348623157E+308)

        Map<Long, TableMapEventBody> tableMaps = new HashMap<>();

        byte[] tableMapHex =
                parseHexBinary("680100000000010015636170747572655F62696E6C6F675F6576656E74730003666F6F000105010801");
        bodyParser.parse(tableMapHex, TABLE_MAP, tableMaps);

        byte[] modifyingEventHex =
                parseHexBinary("6801000000000100020001FFFEFFFFFFFFFFFFEF7F");
        ModifyingEventBody modifyingEventBody = (ModifyingEventBody) bodyParser.parse(modifyingEventHex, EXT_WRITE_ROWS, tableMaps);
        assertThat(modifyingEventBody.getNewRows(), equalTo(ImmutableList.of(new Row("1.7976931348623157E308"))));
    }

    @Test
    public void nullDouble() throws IOException {
        // Query: INSERT INTO capture_binlog_events.foo (nullDouble) VALUES (null)

        Map<Long, TableMapEventBody> tableMaps = new HashMap<>();

        byte[] tableMapHex =
                parseHexBinary("690100000000010015636170747572655F62696E6C6F675F6576656E74730003666F6F000105010801");
        bodyParser.parse(tableMapHex, TABLE_MAP, tableMaps);

        byte[] modifyingEventHex =
                parseHexBinary("6901000000000100020001FFFF");
        ModifyingEventBody modifyingEventBody = (ModifyingEventBody) bodyParser.parse(modifyingEventHex, EXT_WRITE_ROWS, tableMaps);
        assertThat(modifyingEventBody.getNewRows(), equalTo(ImmutableList.of(new Row() {{
            add(null);
        }})));
    }

    @Test
    public void trueBit() throws IOException {
        // Query: INSERT INTO capture_binlog_events.foo (trueBit) VALUES (1)

        Map<Long, TableMapEventBody> tableMaps = new HashMap<>();

        byte[] tableMapHex =
                parseHexBinary("6A0100000000010015636170747572655F62696E6C6F675F6576656E74730003666F6F00011002010001");
        bodyParser.parse(tableMapHex, TABLE_MAP, tableMaps);

        byte[] modifyingEventHex =
                parseHexBinary("6A01000000000100020001FFFE01");
        ModifyingEventBody modifyingEventBody = (ModifyingEventBody) bodyParser.parse(modifyingEventHex, EXT_WRITE_ROWS, tableMaps);
        assertThat(modifyingEventBody.getNewRows(), equalTo(ImmutableList.of(new Row("{0}"))));
    }

    @Test
    public void falseBit() throws IOException {
        // Query: INSERT INTO capture_binlog_events.foo (falseBit) VALUES (0)

        Map<Long, TableMapEventBody> tableMaps = new HashMap<>();

        byte[] tableMapHex =
                parseHexBinary("6B0100000000010015636170747572655F62696E6C6F675F6576656E74730003666F6F00011002010001");
        bodyParser.parse(tableMapHex, TABLE_MAP, tableMaps);

        byte[] modifyingEventHex =
                parseHexBinary("6B01000000000100020001FFFE00");
        ModifyingEventBody modifyingEventBody = (ModifyingEventBody) bodyParser.parse(modifyingEventHex, EXT_WRITE_ROWS, tableMaps);
        assertThat(modifyingEventBody.getNewRows(), equalTo(ImmutableList.of(new Row("{}"))));
    }

    @Test
    public void nullBit() throws IOException {
        // Query: INSERT INTO capture_binlog_events.foo (nullBit) VALUES (null)

        Map<Long, TableMapEventBody> tableMaps = new HashMap<>();

        byte[] tableMapHex =
                parseHexBinary("6C0100000000010015636170747572655F62696E6C6F675F6576656E74730003666F6F00011002010001");
        bodyParser.parse(tableMapHex, TABLE_MAP, tableMaps);

        byte[] modifyingEventHex =
                parseHexBinary("6C01000000000100020001FFFF");
        ModifyingEventBody modifyingEventBody = (ModifyingEventBody) bodyParser.parse(modifyingEventHex, EXT_WRITE_ROWS, tableMaps);
        assertThat(modifyingEventBody.getNewRows(), equalTo(ImmutableList.of(new Row() {{
            add(null);
        }})));
    }

    @Test
    public void tinyintBool() throws IOException {
        // Query: INSERT INTO capture_binlog_events.foo (tinyintBool) VALUES (1)

        Map<Long, TableMapEventBody> tableMaps = new HashMap<>();

        byte[] tableMapHex =
                parseHexBinary("6D0100000000010015636170747572655F62696E6C6F675F6576656E74730003666F6F0001010001");
        bodyParser.parse(tableMapHex, TABLE_MAP, tableMaps);

        byte[] modifyingEventHex =
                parseHexBinary("6D01000000000100020001FFFE01");
        ModifyingEventBody modifyingEventBody = (ModifyingEventBody) bodyParser.parse(modifyingEventHex, EXT_WRITE_ROWS, tableMaps);
        assertThat(modifyingEventBody.getNewRows(), equalTo(ImmutableList.of(new Row("1"))));
    }

    @Test
    public void bitBool() throws IOException {
        // Query: INSERT INTO capture_binlog_events.foo (bitBool) VALUES (1)

        Map<Long, TableMapEventBody> tableMaps = new HashMap<>();

        byte[] tableMapHex =
                parseHexBinary("6E0100000000010015636170747572655F62696E6C6F675F6576656E74730003666F6F00011002010001");
        bodyParser.parse(tableMapHex, TABLE_MAP, tableMaps);

        byte[] modifyingEventHex =
                parseHexBinary("6E01000000000100020001FFFE01");
        ModifyingEventBody modifyingEventBody = (ModifyingEventBody) bodyParser.parse(modifyingEventHex, EXT_WRITE_ROWS, tableMaps);
        assertThat(modifyingEventBody.getNewRows(), equalTo(ImmutableList.of(new Row("{0}"))));
    }

    @Test
    public void minDate() throws IOException {
        // Query: INSERT INTO capture_binlog_events.foo (minDate) VALUES ('1000-01-01')

        Map<Long, TableMapEventBody> tableMaps = new HashMap<>();

        byte[] tableMapHex =
                parseHexBinary("6F0100000000010015636170747572655F62696E6C6F675F6576656E74730003666F6F00010A0001");
        bodyParser.parse(tableMapHex, TABLE_MAP, tableMaps);

        byte[] modifyingEventHex =
                parseHexBinary("6F01000000000100020001FFFE21D007");
        ModifyingEventBody modifyingEventBody = (ModifyingEventBody) bodyParser.parse(modifyingEventHex, EXT_WRITE_ROWS, tableMaps);
        assertThat(modifyingEventBody.getNewRows(), equalTo(ImmutableList.of(new Row("1000-01-01"))));
    }

    @Test
    public void regDate() throws IOException {
        // Query: INSERT INTO capture_binlog_events.foo (regDate) VALUES ('1993-04-15')

        Map<Long, TableMapEventBody> tableMaps = new HashMap<>();

        byte[] tableMapHex =
                parseHexBinary("700100000000010015636170747572655F62696E6C6F675F6576656E74730003666F6F00010A0001");
        bodyParser.parse(tableMapHex, TABLE_MAP, tableMaps);

        byte[] modifyingEventHex =
                parseHexBinary("7001000000000100020001FFFE8F920F");
        ModifyingEventBody modifyingEventBody = (ModifyingEventBody) bodyParser.parse(modifyingEventHex, EXT_WRITE_ROWS, tableMaps);
        assertThat(modifyingEventBody.getNewRows(), equalTo(ImmutableList.of(new Row("1993-04-15"))));
    }

    @Test
    public void maxDate() throws IOException {
        // Query: INSERT INTO capture_binlog_events.foo (maxDate) VALUES ('9999-12-31')

        Map<Long, TableMapEventBody> tableMaps = new HashMap<>();

        byte[] tableMapHex =
                parseHexBinary("710100000000010015636170747572655F62696E6C6F675F6576656E74730003666F6F00010A0001");
        bodyParser.parse(tableMapHex, TABLE_MAP, tableMaps);

        byte[] modifyingEventHex =
                parseHexBinary("7101000000000100020001FFFE9F1F4E");
        ModifyingEventBody modifyingEventBody = (ModifyingEventBody) bodyParser.parse(modifyingEventHex, EXT_WRITE_ROWS, tableMaps);
        assertThat(modifyingEventBody.getNewRows(), equalTo(ImmutableList.of(new Row("9999-12-31"))));
    }

    @Test
    public void nullDate() throws IOException {
        // Query: INSERT INTO capture_binlog_events.foo (nullDate) VALUES (null)

        Map<Long, TableMapEventBody> tableMaps = new HashMap<>();

        byte[] tableMapHex =
                parseHexBinary("720100000000010015636170747572655F62696E6C6F675F6576656E74730003666F6F00010A0001");
        bodyParser.parse(tableMapHex, TABLE_MAP, tableMaps);

        byte[] modifyingEventHex =
                parseHexBinary("7201000000000100020001FFFF");
        ModifyingEventBody modifyingEventBody = (ModifyingEventBody) bodyParser.parse(modifyingEventHex, EXT_WRITE_ROWS, tableMaps);
        assertThat(modifyingEventBody.getNewRows(), equalTo(ImmutableList.of(new Row() {{
            add(null);
        }})));
    }

    // TODO shyiko essentially mods all hour values by 24. We may want to do the same in RowParser#parseTimeV2.
    @Ignore
    @Test
    public void minTime() throws IOException {
        // Query: INSERT INTO capture_binlog_events.foo (minTime) VALUES ('-838:59:59.000000')

        Map<Long, TableMapEventBody> tableMaps = new HashMap<>();

        byte[] tableMapHex =
                parseHexBinary("730100000000010015636170747572655F62696E6C6F675F6576656E74730003666F6F000113010601");
        bodyParser.parse(tableMapHex, TABLE_MAP, tableMaps);

        byte[] modifyingEventHex =
                parseHexBinary("7301000000000100020001FFFE4B9105000000");
        ModifyingEventBody modifyingEventBody = (ModifyingEventBody) bodyParser.parse(modifyingEventHex, EXT_WRITE_ROWS, tableMaps);
        List<Row> newRows = modifyingEventBody.getNewRows();
        assertThat(newRows.size(), equalTo(1));
        // directly test for null value in row to circumvent Objects.requireNonNull check in arraylist constructor
        assertThat(newRows.get(0).get(0), equalTo(null));
    }

    @Test
    public void negTime() throws IOException {
        // Query: INSERT INTO capture_binlog_events.foo (negTime) VALUES ('-01:02:03.000000')

        Map<Long, TableMapEventBody> tableMaps = new HashMap<>();

        byte[] tableMapHex =
                parseHexBinary("E60000000000010015636170747572655F62696E6C6F675F6576656E74730003666F6F000113010601");
        bodyParser.parse(tableMapHex, TABLE_MAP, tableMaps);

        byte[] modifyingEventHex =
                parseHexBinary("E600000000000100020001FFFE7FEF7D000000");
        ModifyingEventBody modifyingEventBody = (ModifyingEventBody) bodyParser.parse(modifyingEventHex, EXT_WRITE_ROWS, tableMaps);
        List<Row> newRows = modifyingEventBody.getNewRows();
        assertThat(newRows.size(), equalTo(1));
        // directly test for null value in row to circumvent Objects.requireNonNull check in arraylist constructor
        assertThat(newRows.get(0).get(0), equalTo(null));
    }

    @Test
    public void negTime2() throws IOException {
        // Query: INSERT INTO capture_binlog_events.foo (negTime) VALUES ('-50:01:02.000000')

        Map<Long, TableMapEventBody> tableMaps = new HashMap<>();

        byte[] tableMapHex =
                parseHexBinary("E70000000000010015636170747572655F62696E6C6F675F6576656E74730003666F6F000113010601");
        bodyParser.parse(tableMapHex, TABLE_MAP, tableMaps);

        byte[] modifyingEventHex =
                parseHexBinary("E700000000000100020001FFFE7CDFBE000000");
        ModifyingEventBody modifyingEventBody = (ModifyingEventBody) bodyParser.parse(modifyingEventHex, EXT_WRITE_ROWS, tableMaps);
        List<Row> newRows = modifyingEventBody.getNewRows();
        assertThat(newRows.size(), equalTo(1));
        // directly test for null value in row to circumvent Objects.requireNonNull check in arraylist constructor
        assertThat(newRows.get(0).get(0), equalTo(null));
    }

    @Test
    public void posTime() throws IOException {
        // Query: INSERT INTO capture_binlog_events.foo (posTime) VALUES ('01:02:03.000000')

        Map<Long, TableMapEventBody> tableMaps = new HashMap<>();

        byte[] tableMapHex =
                parseHexBinary("E80000000000010015636170747572655F62696E6C6F675F6576656E74730003666F6F000113010601");
        bodyParser.parse(tableMapHex, TABLE_MAP, tableMaps);

        byte[] modifyingEventHex =
                parseHexBinary("E800000000000100020001FFFE801083000000");
        ModifyingEventBody modifyingEventBody = (ModifyingEventBody) bodyParser.parse(modifyingEventHex, EXT_WRITE_ROWS, tableMaps);
        assertThat(modifyingEventBody.getNewRows(), equalTo(ImmutableList.of(new Row("01:02:03.000000"))));
    }

    @Test
    public void posTime2() throws IOException {
        // Query: INSERT INTO capture_binlog_events.foo (posTime) VALUES ('50:01:02.000000')

        Map<Long, TableMapEventBody> tableMaps = new HashMap<>();

        byte[] tableMapHex =
                parseHexBinary("E90000000000010015636170747572655F62696E6C6F675F6576656E74730003666F6F000113010601");
        bodyParser.parse(tableMapHex, TABLE_MAP, tableMaps);

        byte[] modifyingEventHex =
                parseHexBinary("E900000000000100020001FFFE832042000000");
        ModifyingEventBody modifyingEventBody = (ModifyingEventBody) bodyParser.parse(modifyingEventHex, EXT_WRITE_ROWS, tableMaps);
        assertThat(modifyingEventBody.getNewRows(), equalTo(ImmutableList.of(new Row("50:01:02.000000"))));
    }

    @Test
    public void regTime() throws IOException {
        // Query: INSERT INTO capture_binlog_events.foo (regTime) VALUES ('12:59:59.000070')

        Map<Long, TableMapEventBody> tableMaps = new HashMap<>();

        byte[] tableMapHex =
                parseHexBinary("740100000000010015636170747572655F62696E6C6F675F6576656E74730003666F6F000113010601");
        bodyParser.parse(tableMapHex, TABLE_MAP, tableMaps);

        byte[] modifyingEventHex =
                parseHexBinary("7401000000000100020001FFFE80CEFB000046");
        ModifyingEventBody modifyingEventBody = (ModifyingEventBody) bodyParser.parse(modifyingEventHex, EXT_WRITE_ROWS, tableMaps);
        assertThat(modifyingEventBody.getNewRows(), equalTo(ImmutableList.of(new Row("12:59:59.000070"))));
    }

    @Test
    public void maxTime() throws IOException {
        // Query: INSERT INTO capture_binlog_events.foo (maxTime) VALUES ('838:59:59.000000')

        Map<Long, TableMapEventBody> tableMaps = new HashMap<>();

        byte[] tableMapHex =
                parseHexBinary("750100000000010015636170747572655F62696E6C6F675F6576656E74730003666F6F000113010601");
        bodyParser.parse(tableMapHex, TABLE_MAP, tableMaps);

        byte[] modifyingEventHex =
                parseHexBinary("7501000000000100020001FFFEB46EFB000000");
        ModifyingEventBody modifyingEventBody = (ModifyingEventBody) bodyParser.parse(modifyingEventHex, EXT_WRITE_ROWS, tableMaps);
        assertThat(modifyingEventBody.getNewRows(), equalTo(ImmutableList.of(new Row("838:59:59.000000"))));
    }

    @Test
    public void nullTime() throws IOException {
        // Query: INSERT INTO capture_binlog_events.foo (nullTime) VALUES (null)

        Map<Long, TableMapEventBody> tableMaps = new HashMap<>();

        byte[] tableMapHex =
                parseHexBinary("760100000000010015636170747572655F62696E6C6F675F6576656E74730003666F6F000113010601");
        bodyParser.parse(tableMapHex, TABLE_MAP, tableMaps);

        byte[] modifyingEventHex =
                parseHexBinary("7601000000000100020001FFFF");
        ModifyingEventBody modifyingEventBody = (ModifyingEventBody) bodyParser.parse(modifyingEventHex, EXT_WRITE_ROWS, tableMaps);
        assertThat(modifyingEventBody.getNewRows(), equalTo(ImmutableList.of(new Row() {{
            add(null);
        }})));
    }

    @Test
    public void minDatetime() throws IOException {
        // Query: INSERT INTO capture_binlog_events.foo (minDatetime) VALUES ('1000-01-01 00:00:00.000001')

        Map<Long, TableMapEventBody> tableMaps = new HashMap<>();

        byte[] tableMapHex =
                parseHexBinary("FB0000000000010015636170747572655F62696E6C6F675F6576656E74730003666F6F000112010601");
        bodyParser.parse(tableMapHex, TABLE_MAP, tableMaps);

        byte[] modifyingEventHex =
                parseHexBinary("FB00000000000100020001FFFE8CB2420000000001");
        ModifyingEventBody modifyingEventBody = (ModifyingEventBody) bodyParser.parse(modifyingEventHex, EXT_WRITE_ROWS, tableMaps);
        assertThat(modifyingEventBody.getNewRows(), equalTo(ImmutableList.of(new Row("1000-01-01 00:00:00.000001"))));
    }

    @Test
    public void regDatetime() throws IOException {
        // Query: INSERT INTO capture_binlog_events.foo (regDatetime) VALUES ('1993-04-15 12:59:59.000007')

        Map<Long, TableMapEventBody> tableMaps = new HashMap<>();

        byte[] tableMapHex =
                parseHexBinary("780100000000010015636170747572655F62696E6C6F675F6576656E74730003666F6F000112010601");
        bodyParser.parse(tableMapHex, TABLE_MAP, tableMaps);

        byte[] modifyingEventHex =
                parseHexBinary("7801000000000100020001FFFE994E5ECEFB000007");
        ModifyingEventBody modifyingEventBody = (ModifyingEventBody) bodyParser.parse(modifyingEventHex, EXT_WRITE_ROWS, tableMaps);
        assertThat(modifyingEventBody.getNewRows(), equalTo(ImmutableList.of(new Row("1993-04-15 12:59:59.000007"))));
    }

    @Test
    public void maxDatetime() throws IOException {
        // Query: INSERT INTO capture_binlog_events.foo (maxDatetime) VALUES ('9999-12-31 23:59:59')

        Map<Long, TableMapEventBody> tableMaps = new HashMap<>();

        byte[] tableMapHex =
                parseHexBinary("790100000000010015636170747572655F62696E6C6F675F6576656E74730003666F6F000112010601");
        bodyParser.parse(tableMapHex, TABLE_MAP, tableMaps);

        byte[] modifyingEventHex =
                parseHexBinary("7901000000000100020001FFFEFEF3FF7EFB000000");
        ModifyingEventBody modifyingEventBody = (ModifyingEventBody) bodyParser.parse(modifyingEventHex, EXT_WRITE_ROWS, tableMaps);
        assertThat(modifyingEventBody.getNewRows(), equalTo(ImmutableList.of(new Row("9999-12-31 23:59:59.000000"))));
    }

    @Test
    public void nullDatetime() throws IOException {
        // Query: INSERT INTO capture_binlog_events.foo (nullDatetime) VALUES (null)

        Map<Long, TableMapEventBody> tableMaps = new HashMap<>();

        byte[] tableMapHex =
                parseHexBinary("7A0100000000010015636170747572655F62696E6C6F675F6576656E74730003666F6F000112010601");
        bodyParser.parse(tableMapHex, TABLE_MAP, tableMaps);

        byte[] modifyingEventHex =
                parseHexBinary("7A01000000000100020001FFFF");
        ModifyingEventBody modifyingEventBody = (ModifyingEventBody) bodyParser.parse(modifyingEventHex, EXT_WRITE_ROWS, tableMaps);
        assertThat(modifyingEventBody.getNewRows(), equalTo(ImmutableList.of(new Row() {{
            add(null);
        }})));
    }

    @Test
    public void minTimestamp() throws IOException {
        // Query: INSERT INTO capture_binlog_events.foo (minTimestamp) VALUES ('1970-01-01 00:00:01.000000')

        Map<Long, TableMapEventBody> tableMaps = new HashMap<>();

        byte[] tableMapHex =
                parseHexBinary("7B0100000000010015636170747572655F62696E6C6F675F6576656E74730003666F6F000111010600");
        bodyParser.parse(tableMapHex, TABLE_MAP, tableMaps);

        byte[] modifyingEventHex =
                parseHexBinary("7B01000000000100020001FFFE00007081000000");
        ModifyingEventBody modifyingEventBody = (ModifyingEventBody) bodyParser.parse(modifyingEventHex, EXT_WRITE_ROWS, tableMaps);
        assertThat(modifyingEventBody.getNewRows(), equalTo(ImmutableList.of(new Row("1970-01-01 00:00:01.000000"))));
    }

    @Test
    public void maxTimestamp() throws IOException {
        // Query: INSERT INTO capture_binlog_events.foo (maxTimestamp) VALUES ('2038-01-18 03:14:07.999999')

        Map<Long, TableMapEventBody> tableMaps = new HashMap<>();

        byte[] tableMapHex =
                parseHexBinary("7C0100000000010015636170747572655F62696E6C6F675F6576656E74730003666F6F000111010600");
        bodyParser.parse(tableMapHex, TABLE_MAP, tableMaps);

        byte[] modifyingEventHex =
                parseHexBinary("7C01000000000100020001FFFE7FFF1EFF0F423F");
        ModifyingEventBody modifyingEventBody = (ModifyingEventBody) bodyParser.parse(modifyingEventHex, EXT_WRITE_ROWS, tableMaps);
        assertThat(modifyingEventBody.getNewRows(), equalTo(ImmutableList.of(new Row("2038-01-18 03:14:07.999999"))));
    }

    @Test
    public void nullTimestamp() throws IOException {
        // Query: INSERT INTO capture_binlog_events.foo (nullTimestamp) VALUES (null)

        Map<Long, TableMapEventBody> tableMaps = new HashMap<>();

        byte[] tableMapHex =
                parseHexBinary("EC0000000000010015636170747572655F62696E6C6F675F6576656E74730003666F6F000111010601");
        bodyParser.parse(tableMapHex, TABLE_MAP, tableMaps);

        byte[] modifyingEventHex =
                parseHexBinary("EC00000000000100020001FFFF");
        ModifyingEventBody modifyingEventBody = (ModifyingEventBody) bodyParser.parse(modifyingEventHex, EXT_WRITE_ROWS, tableMaps);
        assertThat(modifyingEventBody.getNewRows(), equalTo(ImmutableList.of(new Row() {{
            add(null);
        }})));
    }

    @Test
    public void nullJson() throws IOException {
        // Query: INSERT INTO capture_binlog_events.foo (nullJson) VALUES (null)

        Map<Long, TableMapEventBody> tableMaps = new HashMap<>();

        byte[] tableMapHex =
                parseHexBinary("7E0100000000010015636170747572655F62696E6C6F675F6576656E74730003666F6F0001F5010401");
        bodyParser.parse(tableMapHex, TABLE_MAP, tableMaps);

        byte[] modifyingEventHex =
                parseHexBinary("7E01000000000100020001FFFF");
        ModifyingEventBody modifyingEventBody = (ModifyingEventBody) bodyParser.parse(modifyingEventHex, EXT_WRITE_ROWS, tableMaps);
        assertThat(modifyingEventBody.getNewRows(), equalTo(ImmutableList.of(new Row() {{
            add(null);
        }})));
    }

    @Test
    public void intJson() throws IOException {
        // Query: INSERT INTO capture_binlog_events.foo (intJson) VALUES ('1')

        Map<Long, TableMapEventBody> tableMaps = new HashMap<>();

        byte[] tableMapHex =
                parseHexBinary("7F0100000000010015636170747572655F62696E6C6F675F6576656E74730003666F6F0001F5010401");
        bodyParser.parse(tableMapHex, TABLE_MAP, tableMaps);

        byte[] modifyingEventHex =
                parseHexBinary("7F01000000000100020001FFFE03000000050100");
        ModifyingEventBody modifyingEventBody = (ModifyingEventBody) bodyParser.parse(modifyingEventHex, EXT_WRITE_ROWS, tableMaps);
        assertThat(modifyingEventBody.getNewRows(), equalTo(ImmutableList.of(new Row("1"))));
    }

    @Test
    public void trueJson() throws IOException {
        // Query: INSERT INTO capture_binlog_events.foo (trueJson) VALUES ('true')

        Map<Long, TableMapEventBody> tableMaps = new HashMap<>();

        byte[] tableMapHex =
                parseHexBinary("800100000000010015636170747572655F62696E6C6F675F6576656E74730003666F6F0001F5010401");
        bodyParser.parse(tableMapHex, TABLE_MAP, tableMaps);

        byte[] modifyingEventHex =
                parseHexBinary("8001000000000100020001FFFE020000000401");
        ModifyingEventBody modifyingEventBody = (ModifyingEventBody) bodyParser.parse(modifyingEventHex, EXT_WRITE_ROWS, tableMaps);
        assertThat(modifyingEventBody.getNewRows(), equalTo(ImmutableList.of(new Row("true"))));
    }

    @Test
    public void falseJson() throws IOException {
        // Query: INSERT INTO capture_binlog_events.foo (falseJson) VALUES ('false')

        Map<Long, TableMapEventBody> tableMaps = new HashMap<>();

        byte[] tableMapHex =
                parseHexBinary("810100000000010015636170747572655F62696E6C6F675F6576656E74730003666F6F0001F5010401");
        bodyParser.parse(tableMapHex, TABLE_MAP, tableMaps);

        byte[] modifyingEventHex =
                parseHexBinary("8101000000000100020001FFFE020000000402");
        ModifyingEventBody modifyingEventBody = (ModifyingEventBody) bodyParser.parse(modifyingEventHex, EXT_WRITE_ROWS, tableMaps);
        assertThat(modifyingEventBody.getNewRows(), equalTo(ImmutableList.of(new Row("false"))));
    }

    @Test
    public void stringJson() throws IOException {
        // Query: INSERT INTO capture_binlog_events.foo (stringJson) VALUES ('"a"')

        Map<Long, TableMapEventBody> tableMaps = new HashMap<>();

        byte[] tableMapHex =
                parseHexBinary("820100000000010015636170747572655F62696E6C6F675F6576656E74730003666F6F0001F5010401");
        bodyParser.parse(tableMapHex, TABLE_MAP, tableMaps);

        byte[] modifyingEventHex =
                parseHexBinary("8201000000000100020001FFFE030000000C0161");
        ModifyingEventBody modifyingEventBody = (ModifyingEventBody) bodyParser.parse(modifyingEventHex, EXT_WRITE_ROWS, tableMaps);
        assertThat(modifyingEventBody.getNewRows(), equalTo(ImmutableList.of(new Row("\"a\""))));
    }

    @Test
    public void emptyObjectJson() throws IOException {
        // Query: INSERT INTO capture_binlog_events.foo (emptyObjectJson) VALUES ('{}')

        Map<Long, TableMapEventBody> tableMaps = new HashMap<>();

        byte[] tableMapHex =
                parseHexBinary("830100000000010015636170747572655F62696E6C6F675F6576656E74730003666F6F0001F5010401");
        bodyParser.parse(tableMapHex, TABLE_MAP, tableMaps);

        byte[] modifyingEventHex =
                parseHexBinary("8301000000000100020001FFFE050000000000000400");
        ModifyingEventBody modifyingEventBody = (ModifyingEventBody) bodyParser.parse(modifyingEventHex, EXT_WRITE_ROWS, tableMaps);
        assertThat(modifyingEventBody.getNewRows(), equalTo(ImmutableList.of(new Row("{}"))));
    }

    @Test
    public void emptyArrayJson() throws IOException {
        // Query: INSERT INTO capture_binlog_events.foo (emptyArrayJson) VALUES ('[]')

        Map<Long, TableMapEventBody> tableMaps = new HashMap<>();

        byte[] tableMapHex =
                parseHexBinary("840100000000010015636170747572655F62696E6C6F675F6576656E74730003666F6F0001F5010401");
        bodyParser.parse(tableMapHex, TABLE_MAP, tableMaps);

        byte[] modifyingEventHex =
                parseHexBinary("8401000000000100020001FFFE050000000200000400");
        ModifyingEventBody modifyingEventBody = (ModifyingEventBody) bodyParser.parse(modifyingEventHex, EXT_WRITE_ROWS, tableMaps);
        assertThat(modifyingEventBody.getNewRows(), equalTo(ImmutableList.of(new Row("[]"))));
    }

    @Test
    public void basicArrayJson() throws IOException {
        // Query: INSERT INTO capture_binlog_events.foo (basicArrayJson) VALUES ('[1]')

        Map<Long, TableMapEventBody> tableMaps = new HashMap<>();

        byte[] tableMapHex =
                parseHexBinary("850100000000010015636170747572655F62696E6C6F675F6576656E74730003666F6F0001F5010401");
        bodyParser.parse(tableMapHex, TABLE_MAP, tableMaps);

        byte[] modifyingEventHex =
                parseHexBinary("8501000000000100020001FFFE080000000201000700050100");
        ModifyingEventBody modifyingEventBody = (ModifyingEventBody) bodyParser.parse(modifyingEventHex, EXT_WRITE_ROWS, tableMaps);
        assertThat(modifyingEventBody.getNewRows(), equalTo(ImmutableList.of(new Row("[1]"))));
    }

    @Test
    public void complexArrayJson() throws IOException {
        // Query: INSERT INTO capture_binlog_events.foo (complexArrayJson) VALUES ('[1, "a"]')

        Map<Long, TableMapEventBody> tableMaps = new HashMap<>();

        byte[] tableMapHex =
                parseHexBinary("860100000000010015636170747572655F62696E6C6F675F6576656E74730003666F6F0001F5010401");
        bodyParser.parse(tableMapHex, TABLE_MAP, tableMaps);

        byte[] modifyingEventHex =
                parseHexBinary("8601000000000100020001FFFE0D0000000202000C000501000C0A000161");
        ModifyingEventBody modifyingEventBody = (ModifyingEventBody) bodyParser.parse(modifyingEventHex, EXT_WRITE_ROWS, tableMaps);
        assertThat(modifyingEventBody.getNewRows(), equalTo(ImmutableList.of(new Row("[1,\"a\"]"))));
    }

    @Test
    public void basicObjectJson() throws IOException {
        // Query: INSERT INTO capture_binlog_events.foo (basicObjectJson) VALUES ('{"a":false}')

        Map<Long, TableMapEventBody> tableMaps = new HashMap<>();

        byte[] tableMapHex =
                parseHexBinary("870100000000010015636170747572655F62696E6C6F675F6576656E74730003666F6F0001F5010401");
        bodyParser.parse(tableMapHex, TABLE_MAP, tableMaps);

        byte[] modifyingEventHex =
                parseHexBinary("8701000000000100020001FFFE0D0000000001000C000B00010004020061");
        ModifyingEventBody modifyingEventBody = (ModifyingEventBody) bodyParser.parse(modifyingEventHex, EXT_WRITE_ROWS, tableMaps);
        assertThat(modifyingEventBody.getNewRows(), equalTo(ImmutableList.of(new Row("{\"a\":false}"))));
    }

    @Test
    public void nestedArray1Json() throws IOException {
        // Query: INSERT INTO capture_binlog_events.foo (nestedArray1Json) VALUES ('[[1]]')

        Map<Long, TableMapEventBody> tableMaps = new HashMap<>();

        byte[] tableMapHex =
                parseHexBinary("880100000000010015636170747572655F62696E6C6F675F6576656E74730003666F6F0001F5010401");
        bodyParser.parse(tableMapHex, TABLE_MAP, tableMaps);

        byte[] modifyingEventHex =
                parseHexBinary("8801000000000100020001FFFE0F0000000201000E0002070001000700050100");
        ModifyingEventBody modifyingEventBody = (ModifyingEventBody) bodyParser.parse(modifyingEventHex, EXT_WRITE_ROWS, tableMaps);
        assertThat(modifyingEventBody.getNewRows(), equalTo(ImmutableList.of(new Row("[[1]]"))));
    }

    @Test
    public void nestedArray2Json() throws IOException {
        // Query: INSERT INTO capture_binlog_events.foo (nestedArray2Json) VALUES ('[{"a":1}]')

        Map<Long, TableMapEventBody> tableMaps = new HashMap<>();

        byte[] tableMapHex =
                parseHexBinary("890100000000010015636170747572655F62696E6C6F675F6576656E74730003666F6F0001F5010401");
        bodyParser.parse(tableMapHex, TABLE_MAP, tableMaps);

        byte[] modifyingEventHex =
                parseHexBinary("8901000000000100020001FFFE14000000020100130000070001000C000B00010005010061");
        ModifyingEventBody modifyingEventBody = (ModifyingEventBody) bodyParser.parse(modifyingEventHex, EXT_WRITE_ROWS, tableMaps);
        assertThat(modifyingEventBody.getNewRows(), equalTo(ImmutableList.of(new Row("[{\"a\":1}]"))));
    }

    @Test
    public void nestedObject1Json() throws IOException {
        // Query: INSERT INTO capture_binlog_events.foo (nestedObject1Json) VALUES ('{"a": {"b":1}}')

        Map<Long, TableMapEventBody> tableMaps = new HashMap<>();

        byte[] tableMapHex =
                parseHexBinary("8A0100000000010015636170747572655F62696E6C6F675F6576656E74730003666F6F0001F5010401");
        bodyParser.parse(tableMapHex, TABLE_MAP, tableMaps);

        byte[] modifyingEventHex =
                parseHexBinary("8A01000000000100020001FFFE1900000000010018000B000100000C006101000C000B00010005010062");
        ModifyingEventBody modifyingEventBody = (ModifyingEventBody) bodyParser.parse(modifyingEventHex, EXT_WRITE_ROWS, tableMaps);
        assertThat(modifyingEventBody.getNewRows(), equalTo(ImmutableList.of(new Row("{\"a\":{\"b\":1}}"))));
    }

    @Test
    public void nestedObject2Json() throws IOException {
        // Query: INSERT INTO capture_binlog_events.foo (nestedObject2Json) VALUES ('{"a":[0]}')

        Map<Long, TableMapEventBody> tableMaps = new HashMap<>();

        byte[] tableMapHex =
                parseHexBinary("8B0100000000010015636170747572655F62696E6C6F675F6576656E74730003666F6F0001F5010401");
        bodyParser.parse(tableMapHex, TABLE_MAP, tableMaps);

        byte[] modifyingEventHex =
                parseHexBinary("8B01000000000100020001FFFE1400000000010013000B000100020C006101000700050000");
        ModifyingEventBody modifyingEventBody = (ModifyingEventBody) bodyParser.parse(modifyingEventHex, EXT_WRITE_ROWS, tableMaps);
        assertThat(modifyingEventBody.getNewRows(), equalTo(ImmutableList.of(new Row("{\"a\":[0]}"))));
    }
}

