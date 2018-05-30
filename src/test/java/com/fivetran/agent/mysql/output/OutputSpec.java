/**
 * Copyright (c) Fivetran 2018
 **/
package com.fivetran.agent.mysql.output;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fivetran.agent.mysql.Main;
import com.fivetran.agent.mysql.source.BinlogPosition;
import com.fivetran.agent.mysql.source.Row;
import com.fivetran.agent.mysql.source.TableRef;
import com.fivetran.agent.mysql.state.AgentState;
import com.fivetran.agent.mysql.state.TableState;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.fivetran.agent.mysql.output.BucketOutput.DATA_FILE_PREFIX;
import static com.fivetran.agent.mysql.output.BucketOutput.STATE_FILE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

// TODO add test to check contents of state file
public class OutputSpec {
    private Map<File, String> mockOutputFiles = new HashMap<>();
    //    private final int MAX_SIZE = 1024;
    private BucketClient mockClient = new BucketClient() {
        @Override
        public void copy(String prefix, File file) {
            try (FileInputStream in = new FileInputStream(file)) {
                int length = (int) file.length();
                byte[] bytes = new byte[length];
                in.read(bytes);

                mockOutputFiles.compute(file, (key, value) -> (value == null ? "" : value + "\n") + new String(bytes));
                // Files are named using epoch-second timestamps. We must sleep
                // so we don't write to the same file on very quick tests
                Thread.sleep(1000);
            } catch (IOException | InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    };
    private AgentState state = new AgentState();

    {
        state.binlogPosition = new BinlogPosition();
    }

    @Test
    public void writeOut_upsertEvent() throws IOException {
        Row row = new Row("0", "1", "2");
        TableRef tableRef = new TableRef("test_schema", "test_table");

        BucketOutput output = new BucketOutput(mockClient, 1);
        output.emitEvent(Event.createUpsert(tableRef, row), state);

        JsonNode upsertJson = Main.JSON.readValue(getDataFile(), JsonNode.class);
        assertThat(upsertJson.get("upsert").size(), equalTo(3));
        assertThat(upsertJson.get("upsert").get(0).asInt(), equalTo(0));
        assertThat(upsertJson.get("upsert").get(1).asInt(), equalTo(1));
        assertThat(upsertJson.get("upsert").get(2).asInt(), equalTo(2));
    }

    @Test
    public void writeOut_deleteEvent() throws IOException {
        Row row = new Row("0", "1", "2");
        TableRef tableRef = new TableRef("test_schema", "test_table");

        BucketOutput output = new BucketOutput(mockClient, 1);
        output.emitEvent(Event.createDelete(tableRef, row), state);

        JsonNode deleteJson = Main.JSON.readValue(getDataFile(), JsonNode.class);
        assertThat(deleteJson.get("delete").size(), equalTo(3));
        assertThat(deleteJson.get("delete").get(0).asInt(), equalTo(0));
        assertThat(deleteJson.get("delete").get(1).asInt(), equalTo(1));
        assertThat(deleteJson.get("delete").get(2).asInt(), equalTo(2));
    }

    // TODO write test for TableDefinition in state
//    @Test
//    public void writeOut_tableDefinitionEvent() throws IOException {
//        TableRef tableRef = new TableRef("test_schema", "test_table");
//        ImmutableList<ColumnDefinition> columnDefs = ImmutableList.of(new ColumnDefinition("id", "text", true), new ColumnDefinition("data", "text", false));
//        List<String> fromColumns = columnDefs.stream().map(c -> c.name).collect(Collectors.toList());
//        List<String> toColumns = ImmutableList.of("referenced_id", "referenced_data");
//        Map<TableRef, ForeignKey> foreignKeys = ImmutableMap.of(new TableRef("test_schema", "test_referenced_table"), new ForeignKey(fromColumns, toColumns));
//        TableDefinition tableDef = new TableDefinition(tableRef, columnDefs, foreignKeys);
//
//        BucketOutput output = new BucketOutput(mockClient, 1);
//        output.emitEvent(Event.createTableDefinition(tableDef), state);
//        JsonNode tableDefJson = Main.JSON.readValue(getDataFile(), JsonNode.class);
//
//        ArrayNode columnsJson = (ArrayNode) tableDefJson.get("tableDefinition");
//        assertThat(tableDefJson.get("table").get("schema").asText(), equalTo("test_schema"));
//        assertThat(tableDefJson.get("table").get("name").asText(), equalTo("test_table"));
//
//        assertThat(columnsJson.get(0).get("name").asText(), equalTo("id"));
//        assertThat(columnsJson.get(0).get("type").asText(), equalTo("text"));
//        assertTrue(columnsJson.get(0).get("key").asBoolean());
//
//        assertThat(columnsJson.get(1).get("name").asText(), equalTo("data"));
//        assertThat(columnsJson.get(1).get("type").asText(), equalTo("text"));
//        assertFalse(columnsJson.get(1).get("key").asBoolean());
//
//        ArrayNode fromColumnsJson = (ArrayNode) tableDefJson.get("foreignKeys").get("test_schema.test_referenced_table").get("columns");
//        assertThat(fromColumnsJson.size(), equalTo(2));
//        assertThat(fromColumnsJson.get(0).asText(), equalTo("id"));
//        assertThat(fromColumnsJson.get(1).asText(), equalTo("data"));
//
//        ArrayNode toColumnsJson = (ArrayNode) tableDefJson.get("foreignKeys").get("test_schema.test_referenced_table").get("referencedColumns");
//        assertThat(toColumnsJson.size(), equalTo(2));
//        assertThat(toColumnsJson.get(0).asText(), equalTo("referenced_id"));
//        assertThat(toColumnsJson.get(1).asText(), equalTo("referenced_data"));
//
//
//    }

    @Test
    public void writeOut_writeOnlyWhenMaxSizeExceeded() {
        BucketOutput output = new BucketOutput(mockClient, 100);
        TableRef tableRef = new TableRef("test_schema", "test_table");
        ImmutableList<ColumnDefinition> columnDefs = ImmutableList.of(new ColumnDefinition("id", "text", true), new ColumnDefinition("data", "text", false));
        TableDefinition tableDef = new TableDefinition(tableRef, columnDefs);

        Row row = new Row("0", "1", "2");

        output.emitEvent(Event.createUpsert(tableRef, row), state);

        assertTrue(mockOutputFiles.isEmpty());

        output.emitEvent(Event.createUpsert(tableRef, row), state);

        assertFalse(mockOutputFiles.isEmpty());
    }

    @Test
    public void writeOut_alwaysWriteOnFinish() {
        try (BucketOutput output = new BucketOutput(mockClient, 1_000_000)) {
            TableRef tableRef = new TableRef("test_schema", "test_table");
            ImmutableList<ColumnDefinition> columnDefs = ImmutableList.of(new ColumnDefinition("id", "text", true), new ColumnDefinition("data", "text", false));

            Row row = new Row("0", "1", "2");

            output.emitEvent(Event.createUpsert(tableRef, row), state);
            output.emitEvent(Event.createUpsert(tableRef, row), state);

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        assertFalse(mockOutputFiles.isEmpty());
    }

    @Test
    public void writeOut_state() throws IOException {
        BucketOutput output = new BucketOutput(mockClient, 1);
        TableRef tableRef = new TableRef("test_schema", "test_table");

        TableState tableState = new TableState();
        tableState.finishedImport = true;
        state.tableStates.put(tableRef, new TableState());
        state.binlogPosition = new BinlogPosition("mysql-bin.000001", 154);

        output.emitEvent(Event.createUpsert(tableRef, new Row()), state);
        AgentState outputtedState = Main.JSON.readValue(getStateFile(), AgentState.class);

        assertTrue(outputtedState.equals(state));
    }

    private String getDataFile() {
        return mockOutputFiles.keySet().stream()
                .filter(file -> file.getPath().startsWith(DATA_FILE_PREFIX))
                .map(f -> mockOutputFiles.get(f))
                .findFirst()
                .orElseThrow(RuntimeException::new);
    }

    private String getStateFile() {
        return mockOutputFiles.keySet().stream()
                .filter(file -> file.getPath().equals(STATE_FILE))
                .map(f -> mockOutputFiles.get(f))
                .findFirst()
                .orElseThrow(RuntimeException::new);
    }
}
