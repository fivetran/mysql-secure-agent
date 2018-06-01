package com.fivetran.agent.mysql.serialize;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fivetran.agent.mysql.output.ColumnDefinition;
import com.fivetran.agent.mysql.output.ForeignKey;
import com.fivetran.agent.mysql.output.TableDefinition;
import com.fivetran.agent.mysql.source.BinlogPosition;
import com.fivetran.agent.mysql.source.TableRef;
import com.fivetran.agent.mysql.state.AgentState;
import com.fivetran.agent.mysql.state.TableState;
import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Optional;

import static junit.framework.TestCase.assertTrue;

public class SerializeTest {
    @Test
    public void serializeAgentState() {
        String correctResult = "{\n" +
                " \"binlogPosition\": {\n" +
                "  \"file\": \"some_binlog_file\",\n" +
                "  \"position\": 1\n" +
                " },\n" +
                " \"tableStates\": {\n" +
                "  \"schema_one.table_one\": {\n" +
                "   \"lastSyncedPrimaryKey\": {\n" +
                "    \"table_pkey\": \"pkey_value\"\n" +
                "   },\n" +
                "   \"finishedImport\": true\n" +
                "  }\n" +
                " },\n" +
                " \"tableDefinitions\": {\n" +
                "  \"schema_one.table_one\": {\n" +
                "   \"table\": \"schema_one.table_one\", \n" +
                "   \"foreignKeys\": {\n" +
                "    \"foreign_schema.foreign_table\": {\n" +
                "     \"columns\": [\"table_fkey\"],\n" +
                "     \"referencedColumns\": [\"foreign_table_pkey\"]\n" +
                "    }\n" +
                "   },\n" +
                "   \"tableDefinition\": [{\n" +
                "    \"name\": \"table_pkey\",\n" +
                "    \"type\": \"text\",\n" +
                "    \"key\": true\n" +
                "   }, {\n" +
                "    \"name\": \"table_fkey\",\n" +
                "    \"type\": \"text\",\n" +
                "    \"key\": false\n" +
                "   }]\n" +
                "  }\n" +
                " }\n" +
                "}";

        AgentState state = new AgentState();
        TableRef tableRef = new TableRef("schema_one", "table_one");

        TableState tableState = new TableState();
        tableState.finishedImport = true;
        tableState.lastSyncedPrimaryKey = Optional.of(ImmutableMap.of("table_pkey", "pkey_value"));

        TableDefinition tableDef = new TableDefinition();
        tableDef.columns = new ArrayList<>();
        tableDef.columns.add(new ColumnDefinition("table_pkey", "text", true));
        tableDef.columns.add(new ColumnDefinition("table_fkey", "text", false));
        tableDef.table = tableRef;
        tableDef.foreignKeys = ImmutableMap.of(new TableRef("foreign_schema", "foreign_table"), new ForeignKey("table_fkey", "foreign_table_pkey"));

        state.binlogPosition = new BinlogPosition("some_binlog_file", 1);
        state.tableStates.put(tableRef, tableState);
        state.tableDefinitions.put(tableRef, tableDef);

        assertTrue(correctResult.equals(Serialize.value(state)));

    }
}
