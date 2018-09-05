package com.fivetran.agent.mysql.serialize;

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

import static junit.framework.TestCase.assertEquals;

public class SerializeTest {
    @Test
    public void serializeAgentState() {
        String correctResult = "{\n" +
                "  \"binlogPosition\" : {\n" +
                "    \"file\" : \"some_binlog_file\",\n" +
                "    \"position\" : 1234567890\n" +
                "  },\n" +
                "  \"tableStates\" : {\n" +
                "    \"primary_schema.primary_table\" : {\n" +
                "      \"lastSyncedPrimaryKey\" : {\n" +
                "        \"primary_pkey\" : \"primary_pkey_value\"\n" +
                "      },\n" +
                "      \"finishedImport\" : true\n" +
                "    },\n" +
                "    \"foreign_schema.foreign_table\" : {\n" +
                "      \"lastSyncedPrimaryKey\" : null,\n" +
                "      \"finishedImport\" : false\n" +
                "    }\n" +
                "  },\n" +
                "  \"tableDefinitions\" : {\n" +
                "    \"primary_schema.primary_table\" : {\n" +
                "      \"table\" : \"primary_schema.primary_table\",\n" +
                "      \"foreignKeys\" : {\n" +
                "        \"foreign_schema.foreign_table\" : {\n" +
                "          \"columns\" : [ \"primary_fkey\" ],\n" +
                "          \"referencedColumns\" : [ \"foreign_pkey\" ]\n" +
                "        }\n" +
                "      },\n" +
                "      \"tableDefinition\" : [ {\n" +
                "        \"name\" : \"primary_pkey\",\n" +
                "        \"type\" : \"text\",\n" +
                "        \"key\" : true\n" +
                "      }, {\n" +
                "        \"name\" : \"primary_fkey\",\n" +
                "        \"type\" : \"text\",\n" +
                "        \"key\" : false\n" +
                "      } ]\n" +
                "    },\n" +
                "    \"foreign_schema.foreign_table\" : {\n" +
                "      \"table\" : \"foreign_schema.foreign_table\",\n" +
                "      \"foreignKeys\" : null,\n" +
                "      \"tableDefinition\" : [ {\n" +
                "        \"name\" : \"foreign_pkey\",\n" +
                "        \"type\" : \"text\",\n" +
                "        \"key\" : true\n" +
                "      } ]\n" +
                "    }\n" +
                "  }\n" +
                "}";

        TableRef primaryTableRef = new TableRef("primary_schema", "primary_table");
        TableRef foreignTableRef = new TableRef("foreign_schema", "foreign_table");

        TableState primaryTableState = new TableState();
        primaryTableState.finishedImport = true;
        primaryTableState.lastSyncedPrimaryKey = Optional.of(ImmutableMap.of("primary_pkey", "primary_pkey_value"));

        TableState referencedTableState = new TableState();
        referencedTableState.finishedImport = false;
        referencedTableState.lastSyncedPrimaryKey = Optional.empty();

        TableDefinition primaryTableDef = new TableDefinition();
        primaryTableDef.columns = new ArrayList<>();
        primaryTableDef.columns.add(new ColumnDefinition("primary_pkey", "text", true));
        primaryTableDef.columns.add(new ColumnDefinition("primary_fkey", "text", false));
        primaryTableDef.table = primaryTableRef;
        primaryTableDef.foreignKeys = ImmutableMap.of(new TableRef("foreign_schema", "foreign_table"), new ForeignKey("primary_fkey", "foreign_pkey"));

        TableDefinition referencedTableDef = new TableDefinition();
        referencedTableDef.columns = new ArrayList<>();
        referencedTableDef.columns.add(new ColumnDefinition("foreign_pkey", "text", true));
        referencedTableDef.table = foreignTableRef;

        AgentState state = new AgentState();
        state.binlogPosition = new BinlogPosition("some_binlog_file", 1234567890L);
        state.tableStates.put(primaryTableRef, primaryTableState);
        state.tableStates.put(foreignTableRef, referencedTableState);
        state.tableDefinitions.put(primaryTableRef, primaryTableDef);
        state.tableDefinitions.put(foreignTableRef, referencedTableDef);

        System.out.println(Serialize.value(state));

        assertEquals(correctResult, Serialize.value(state));
    }
}
