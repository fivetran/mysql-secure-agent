/**
 * Copyright (c) Fivetran 2018
 **/
package com.fivetran.agent.mysql.source;

import com.fivetran.agent.mysql.Rows;
import com.fivetran.agent.mysql.output.TableDefinition;
import com.fivetran.agent.mysql.utils.AgentUtils;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TableDefinitionsTest {
    @Test
    public void get_oneColumn() {
        Query query = new Query() {
            @Override
            public Rows unlimitedRows(String query) {
                return null;
            }

            @Override
            public List<Record> records(String query) {
                List<Record> records = new ArrayList<>();
                Record record = new Record(10);
                Map<String, String> recordValues = AgentUtils.map(
                        ImmutableList.of("TABLE_SCHEMA", "TABLE_NAME", "COLUMN_NAME", "ORDINAL_POSITION", "COLUMN_TYPE", "CHARACTER_SET_NAME", "COLUMN_KEY", "REFERENCED_TABLE_SCHEMA", "REFERENCED_TABLE_NAME", "REFERENCED_COLUMN_NAME"),
                        ImmutableList.of("test_schema", "test_table", "id", "1", "int", "latin1", "PRI", "test_schema", "reference_table", "reference_column"));
                record.putAll(recordValues);
                records.add(record);
                return records;
            }

            @Override
            public Record record(String query) {
                return null;
            }

            @Override
            public BinlogPosition target() {
                return null;
            }
        };

        TableDefinitions tableDefinitions = new TableDefinitions(query);
        Map<TableRef, TableDefinition> tables = tableDefinitions.get();

        assertTrue(tables.containsKey(new TableRef("test_schema", "test_table")));
        assertTrue(tables.get(new TableRef("test_schema", "test_table")).columns.size() == 1);
        assertTrue(tables.get(new TableRef("test_schema", "test_table")).columns.get(0).name.equals("id"));
        assertTrue(tables.get(new TableRef("test_schema", "test_table")).columns.get(0).key);
        assertTrue(tables.get(new TableRef("test_schema", "test_table")).columns.get(0).type.equals("int"));
    }

    @Test
    public void get_multiPartForeignKey() {
        Query query = new Query() {
            @Override
            public Rows unlimitedRows(String query) {
                return null;
            }

            @Override
            public List<Record> records(String query) {
                ImmutableList<String> columns = ImmutableList.of("TABLE_SCHEMA", "TABLE_NAME", "COLUMN_NAME", "ORDINAL_POSITION", "COLUMN_TYPE", "CHARACTER_SET_NAME", "COLUMN_KEY", "REFERENCED_TABLE_SCHEMA", "REFERENCED_TABLE_NAME", "REFERENCED_COLUMN_NAME");
                List<Record> records = ImmutableList.of(
                        new Record(AgentUtils.map(columns,
                                ImmutableList.of("test_schema", "test_table", "id", "1", "int", "latin1", "PRI", "test_schema", "reference_table_1", "reference_id")
                        )),
                        new Record(AgentUtils.map(columns,
                                ImmutableList.of("test_schema", "test_table", "from_reference_1", "2", "int", "latin1", "", "test_schema", "reference_table_1", "to_reference_1")
                        )),
                        new Record(AgentUtils.map(columns,
                                ImmutableList.of("test_schema", "test_table", "id", "1", "int", "latin1", "PRI", "test_schema", "reference_table_2", "reference_id")
                        )),
                        new Record(AgentUtils.map(columns,
                                ImmutableList.of("test_schema", "test_table", "from_reference_2", "3", "int", "latin1", "", "test_schema", "reference_table", "to_reference_2"))
                        ));

                return records;
            }

            @Override
            public Record record(String query) {
                return null;
            }

            @Override
            public BinlogPosition target() {
                return null;
            }
        };

        TableDefinitions tableDefinitions = new TableDefinitions(query);
        Map<TableRef, TableDefinition> tables = tableDefinitions.get();
        TableDefinition tableDefinition = tables.get(new TableRef("test_schema", "test_table"));
        tableDefinition.columns.sort(Comparator.comparing(c -> c.name));

        assertTrue(tables.containsKey(new TableRef("test_schema", "test_table")));

        assertTrue(tableDefinition.columns.size() == 3);

        assertTrue(tableDefinition.columns.get(0).name.equals("from_reference_1"));
        assertFalse(tableDefinition.columns.get(0).key);
        assertTrue(tableDefinition.columns.get(0).type.equals("int"));

        assertTrue(tableDefinition.columns.get(1).name.equals("from_reference_2"));
        assertFalse(tableDefinition.columns.get(1).key);
        assertTrue(tableDefinition.columns.get(1).type.equals("int"));


        assertTrue(tableDefinition.columns.get(2).name.equals("id"));
        assertTrue(tableDefinition.columns.get(2).key);
        assertTrue(tableDefinition.columns.get(2).type.equals("int"));

        TableRef referenceTable = new TableRef("test_schema", "reference_table");
        assertTrue(tableDefinition.foreignKeys.get(referenceTable).referencedColumns.contains("to_reference_2"));
        assertTrue(tableDefinition.foreignKeys.get(referenceTable).columns.contains("from_reference_2"));

        TableRef referenceTable1 = new TableRef("test_schema", "reference_table_1");
        assertTrue(tableDefinition.foreignKeys.get(referenceTable1).referencedColumns.containsAll(ImmutableList.of("reference_id", "to_reference_1")));
        assertTrue(tableDefinition.foreignKeys.get(referenceTable1).columns.containsAll(ImmutableList.of("from_reference_1", "id")));

        TableRef referenceTable2 = new TableRef("test_schema", "reference_table_2");
        assertTrue(tableDefinition.foreignKeys.get(referenceTable2).referencedColumns.contains("reference_id"));
        assertTrue(tableDefinition.foreignKeys.get(referenceTable2).columns.contains("id"));
    }
}