package com.fivetran.agent.mysql.config;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fivetran.agent.mysql.Updater;
import com.fivetran.agent.mysql.output.ColumnDefinition;
import com.fivetran.agent.mysql.output.TableDefinition;
import com.fivetran.agent.mysql.source.TableRef;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class ConfigSpec {

    // TODO test this!
    @Ignore
    public void configSerialization() throws JsonProcessingException {
        Config config = new Config();
        TableRef tableRef = new TableRef("test_config_schema", "test_config_table");
        config.schemas.put(tableRef.schemaName, new SchemaConfig());
        config.schemas.get(tableRef.schemaName).tables.put(tableRef.tableName, new TableConfig());
        config.schemas.get(tableRef.schemaName).tables.get(tableRef.tableName).columns.put("test_config_column", new ColumnConfig());
        config.schemas.get(tableRef.schemaName).tables.get(tableRef.tableName).columns.put("unsynced_config_column", new ColumnConfig());
        config.schemas.get(tableRef.schemaName).tables.get(tableRef.tableName).columns.get("unsynced_config_column").selected = false;

        String configSerialized = Updater.JSON.writerWithDefaultPrettyPrinter().writeValueAsString(config);

        String target = "{\n" +
                "  \"schemas\" : {\n" +
                "    \"test_config_schema\" : {\n" +
                "      \"selected\" : true,\n" +
                "      \"tables\" : {\n" +
                "        \"test_config_table\" : {\n" +
                "          \"selected\" : true,\n" +
                "          \"columns\" : {\n" +
                "            \"unsynced_config_column\" : {\n" +
                "              \"selected\" : false,\n" +
                "              \"hash\" : false,\n" +
                "              \"implicitKey\" : {\n" +
                "                \"present\" : false\n" +
                "              }\n" +
                "            },\n" +
                "            \"test_config_column\" : {\n" +
                "              \"selected\" : true,\n" +
                "              \"hash\" : false,\n" +
                "              \"implicitKey\" : {\n" +
                "                \"present\" : false\n" +
                "              }\n" +
                "            }\n" +
                "          },\n" +
                "          \"selectOtherColumns\" : true\n" +
                "        }\n" +
                "      },\n" +
                "      \"selectOtherTables\" : true\n" +
                "    }\n" +
                "  },\n" +
                "  \"selectOtherSchemas\" : true\n" +
                "}";

        assertEquals(configSerialized, target);
    }

    @Test
    public void configDeserialization() {

    }

    @Test
    public void getTablesToSync_schemaConfigDisabledSelectOtherTables() {
        TableRef syncedTable = new TableRef("test_schema", "select_table");
        TableRef ignoredTable = new TableRef("test_schema", "ignored_table");
        Config config = new Config();

        SchemaConfig schemaConfig = new SchemaConfig();
        schemaConfig.selectOtherTables = false;
        config.schemas.put("test_schema", schemaConfig);

        TableConfig tableConfig = new TableConfig();
        tableConfig.selected = true;
        config.getSchema(syncedTable.schemaName).get().tables.put(syncedTable.tableName, tableConfig);
        Map<TableRef, TableDefinition> tableDefinitions = new HashMap<>();
        tableDefinitions.put(syncedTable, new TableDefinition(syncedTable, Arrays.asList(new ColumnDefinition("id", "text", true), new ColumnDefinition("data", "text", false))));
        tableDefinitions.put(ignoredTable, new TableDefinition(ignoredTable, Arrays.asList(new ColumnDefinition("id", "text", true), new ColumnDefinition("data", "text", false))));


        Map<TableRef, TableDefinition>  tablesToSync = config.getTablesToSync(tableDefinitions);

        assertNotNull(tablesToSync.get(syncedTable));
        assertEquals(tablesToSync.size(), 1);
    }

    @Test
    public void getTablesToSync_schemaConfigEnabledSelectOtherTables() {
        TableRef explicitlySelectedTable = new TableRef("test_schema", "explicit_table");
        TableRef implicitlySelectedTable = new TableRef("test_schema", "implicit_table");

        Config config = new Config();

        SchemaConfig schemaConfig = new SchemaConfig();
        config.schemas.put("test_schema", schemaConfig);

        TableConfig tableConfig = new TableConfig();
        tableConfig.selected = true;
        config.getSchema(explicitlySelectedTable.schemaName).get().tables.put(explicitlySelectedTable.tableName, tableConfig);
        Map<TableRef, TableDefinition> tableDefinitions = new HashMap<>();
        tableDefinitions.put(explicitlySelectedTable, new TableDefinition(explicitlySelectedTable, Arrays.asList(new ColumnDefinition("id", "text", true), new ColumnDefinition("data", "text", false))));
        tableDefinitions.put(implicitlySelectedTable, new TableDefinition(implicitlySelectedTable, Arrays.asList(new ColumnDefinition("id", "text", true), new ColumnDefinition("data", "text", false))));


        Map<TableRef, TableDefinition>  tablesToSync = config.getTablesToSync(tableDefinitions);

        assertNotNull(tablesToSync.get(explicitlySelectedTable));
        assertNotNull(tablesToSync.get(implicitlySelectedTable));
        assertEquals(tablesToSync.size(), 2);
    }

    @Test
    public void getTablesToSync_ignoreUnselectedSchema() {
        Config config = new Config();

        TableRef tableInSelectedSchema = new TableRef("selected_schema", "test_table");
        TableRef tableInIgnoredSchema = new TableRef("ignored_schema", "test_table");

        SchemaConfig schemaConfig = new SchemaConfig();
        schemaConfig.selectOtherTables = false;
        config.schemas.put("ignored_schema", schemaConfig);

        Map<TableRef, TableDefinition> tableDefinitions = new HashMap<>();
        tableDefinitions.put(tableInIgnoredSchema, new TableDefinition(tableInIgnoredSchema, Arrays.asList(new ColumnDefinition("id", "text", true), new ColumnDefinition("data", "text", false))));
        tableDefinitions.put(tableInSelectedSchema, new TableDefinition(tableInSelectedSchema, Arrays.asList(new ColumnDefinition("id", "text", true), new ColumnDefinition("data", "text", false))));


        Map<TableRef, TableDefinition>  tablesToSync = config.getTablesToSync(tableDefinitions);

        assertEquals(tablesToSync.size(), 1);
        assertNotNull(tablesToSync.get(tableInSelectedSchema));
    }

    @Test
    public void getColumnsToSync_ignoreUnselectedColumn() {
        TableRef tableRef = new TableRef("test_schema", "test_table");

        Config config = new Config();
        config.putColumn(config, tableRef, "selected_column", true, false);
        config.putColumn(config, tableRef, "unselected_column", false, false);
        TableDefinition tableDef = new TableDefinition(tableRef, Arrays.asList(new ColumnDefinition("selected_column", "text", true), new ColumnDefinition("unselected_column", "text", false)));

        List<ColumnDefinition> columnsToSync = config.getColumnsToSync(tableDef);

        assertEquals(columnsToSync.size(), 1);
        assertEquals(columnsToSync.get(0), new ColumnDefinition("selected_column", "text", true));
    }

    @Test
    public void getColumnsToSync_tableConfigDisabledSelectOtherColumns() {
        TableRef tableRef = new TableRef("test_schema", "test_table");

        Config config = new Config();
        config.schemas.put(tableRef.schemaName, new SchemaConfig());
        config.schemas.get(tableRef.schemaName).tables.put(tableRef.tableName, new TableConfig(true, false));
        config.putColumn(config, tableRef, "selected_column", true, false);

        TableDefinition tableDef = new TableDefinition(tableRef, Arrays.asList(new ColumnDefinition("selected_column", "text", true), new ColumnDefinition("unselected_column", "text", false)));

        List<ColumnDefinition> columnsToSync = config.getColumnsToSync(tableDef);

        assertEquals(columnsToSync.size(), 1);
        assertEquals(columnsToSync.get(0), new ColumnDefinition("selected_column", "text", true));
    }
}
