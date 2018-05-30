/**
* Copyright (c) Fivetran 2018
**/
package com.fivetran.agent.mysql.binlog_test_generator;

import com.fivetran.agent.mysql.source.binlog.parser.EventType;

import java.io.*;

public class TestEvent {

    // todo: likely move this along with related classes to another repo once tests no longer need to be generated

    private EventType type;
    private byte[] header;
    private byte[] body;
    private String query;
    private String column;
    private String oldValue;
    private String newValue;
    private TestEvent tableMap;

    private TestEvent(EventType type, byte[] header, byte[] body) {
        this.type = type;
        this.header = header;
        this.body = body;
    }

    private TestEvent(int type, byte[] header, byte[] body) {
        this(EventType.values()[type], header, body);
    }

    TestEvent(int type, byte[] header, byte[] body, String column, String query) {
        this(type, header, body);
        this.column = column;
        this.query = query;
    }

    public EventType getType() {
        return type;
    }

    public byte[] getHeader() {
        return header;
    }

    private byte[] getBody() {
        return body;
    }

    public void setType(EventType type) {
        this.type = type;
    }

    public void setHeader(byte[] header) {
        this.header = header;
    }

    public void setBody(byte[] body) {
        this.body = body;
    }

    public String getQuery() {
        return query;
    }

    public void setQuery(String query) {
        this.query = query;
    }

    public String getColumn() {
        return column;
    }

    public void setColumn(String column) {
        this.column = column;
    }

    public String getOldValue() {
        return oldValue;
    }

    public void setOldValue(String oldValue) {
        this.oldValue =
                oldValue == null
                        ? null
                        : oldValue
                        .replaceAll("'", "\"");
//                        .replaceAll("\"", "\\\\\"");
    }

    public void setNewValue(String newValue) {
        this.newValue =
                newValue == null
                        ? null
                        : newValue
                        .replaceAll("'", "\"");
    }

    void appendQuery(StringBuilder sb) {
        sb.append("    @Test\n");
        sb.append("    public void " + column + "() throws IOException {\n");
        sb.append("        // Query: " + (query.length() < 100 ? query : query.substring(0, 99) + "...')") + "\n\n");
    }

    void printFullEventHex() {
        String statement = "       byte[] bytes = parseHexBinary(\"" + getHex(header) + getHex(body) + "\");\n";
        System.out.println(statement);
    }

    void printHeader() {
        StringBuilder sb = new StringBuilder();

        appendQuery(sb);

        sb.append("        byte[] header = parseHexBinary(\"" + getHex(header) + "\");\n\n");
        sb.append("        EventHeader eventHeader = headerParser.parse(header);\n");
        sb.append("        // assertThat(eventHeader.getTimestamp(), equalTo(calculate value)); // todo\n");
        sb.append("        assertThat(eventHeader.getType(), equalTo(" + type + "));\n");
        sb.append("        assertThat(eventHeader.getServerId(), equalTo(1L));\n");
        sb.append("        // assertThat(eventHeader.getEventLength(), equalTo(calculate value)); // todo\n");
        sb.append("        // assertThat(eventHeader.getBodyLength(), equalTo(caluclate value)); // todo\n");
        sb.append("        assertThat(eventHeader.getHeaderLength(), equalTo(19L));\n");
        sb.append("        // assertThat(eventHeader.getNextPosition(), equalTo(calculate value)); // todo\n");
        sb.append("        assertThat(eventHeader.getFlags(), equalTo(0));\n");
        sb.append("    }\n");
        System.out.println(sb.toString());
    }

    private void appendTableMapBody(StringBuilder sb) {
        sb.append("        Map<Long, TableMapEventBody> tableMaps = new HashMap<>();\n\n");
        sb.append("        byte[] tableMapHex = \nparseHexBinary(\"" + getHex(tableMap.getBody()) + "\");\n");
        sb.append("        bodyParser.parse(tableMapHex, TABLE_MAP, tableMaps);\n\n");
    }

    void printMassive() {
        StringBuilder sb = new StringBuilder();

        appendQuery(sb);
        appendTableMapBody(sb);

        writeToFile();
        sb.append("        String hexInput = " +
                "IOUtils.toString(new FileInputStream(new File(\"src/test/test_resources/massive-hex.txt\")));\n");

        sb.append("        byte[] modifyingEventHex = parseHexBinary(" + "hexInput" + ");\n");
        sb.append("        String desiredResult = "
                + "IOUtils.toString(new FileInputStream(new File(\"src/test/test_resources/sixteen-mb-string.txt\")));\n"
        );
        sb.append("        ModifyingEventBody modifyingEventBody = (ModifyingEventBody) bodyParser.parse(modifyingEventHex, EXT_WRITE_ROWS, tableMaps);\n");
        sb.append("        assertThat(modifyingEventBody.getNewRows(), equalTo(ImmutableList.of(new Row(" +
                "desiredResult)));\n");
        sb.append("    }\n");
        System.out.println(sb.toString());
    }

    void printWriteBody(String value) {
        StringBuilder sb = new StringBuilder();

        appendQuery(sb);
        appendTableMapBody(sb);

        sb.append("        byte[] modifyingEventHex = \nparseHexBinary(" + "\"" + getHex(body) + "\"" + ");\n");
        sb.append("        ModifyingEventBody modifyingEventBody = (ModifyingEventBody) bodyParser.parse(modifyingEventHex, EXT_WRITE_ROWS, tableMaps);\n");
        sb.append("        assertThat(modifyingEventBody.getNewRows(), equalTo(ImmutableList.of(new Row(" +
                value.replaceAll("\"", "\\\\\\\"")
                        .replaceAll("'", "\"")
                        .replaceAll(" ", "") +
                "))));\n");
        sb.append("    }\n");
        System.out.println(sb.toString());
    }

    private void writeToFile() {
        try {
            Writer writer = new PrintWriter("src/test/test_resources/massive-hex.txt");
            BufferedWriter bufferedWriter = new BufferedWriter(writer);
            bufferedWriter.write(getHex(body));
            bufferedWriter.flush();
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // todo: this will need fixing
    void printTableMap() {
        StringBuilder sb = new StringBuilder();

        appendQuery(sb);
        appendTableMapBody(sb);

        sb.append("        assertThat(tableMapEventBody.getTableId(), equalTo(" + getTableId() + "L));\n");
        sb.append("        assertThat(tableMapEventBody.getTableRef().schema, equalTo(\"capture_binlog_events\"));\n");
        sb.append("        assertThat(tableMapEventBody.getTableRef().name, equalTo(\"foo\"));\n");
        sb.append("        assertThat(tableMapEventBody.getColumnTypes(), equalTo(new byte[] {0})); // todo: add appropriate types\n");
        sb.append("    }\n");
        System.out.println(sb.toString());
    }

    private String getHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes)
            sb.append(String.format("%02X", b));
        return sb.toString();
    }

    private int getTableId() {
        int result = 0;

        for (int i = 0; i < 6; ++i)
            result |= ((int) body[i] << (i << 3));

        return result;
    }

    public void setTableMap(TestEvent tableMap) {
        this.tableMap = tableMap;
    }
}
