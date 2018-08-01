/**
* Copyright (c) Fivetran 2018
**/
package com.fivetran.agent.mysql.source;

import com.fivetran.agent.mysql.output.ColumnDefinition;
import com.fivetran.agent.mysql.output.ForeignKey;
import com.fivetran.agent.mysql.output.TableDefinition;
import com.google.common.collect.ImmutableList;

import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class TableDefinitions implements Supplier<Map<TableRef, TableDefinition>> {

    private final Query query;

    public TableDefinitions(Query query) {
        this.query = query;
    }

    /**
     * Carries out a query against the source database, which enumerates all columns and all relevant information
     * about each column, such as names, table name, schema name, ordinal position, and any potential foreign key
     * references.
     *
     * Note - Mysql column ordinal positions are 1-indexed, which is why we must subtract one
     * from the ordinal position when inserting into the list containing all ColumnDefinitions.
     *
     * Multiple ColumnAttributes can exist for the same column. For instance, if a column has foreign keys
     * pointing to two locations, it will have two ColumnAttributes which differ only in their "referencedColumn"
     * field. Because we've already processed the referenced columns, we can grab the first column attribute without
     * considering the others, as all other values are identical.
     * @return a map from all tables to their complete set of columns.
     */
    @Override
    public Map<TableRef, TableDefinition> get() {
        Map<TableRef, TableDefinition> tables = new HashMap<>();
        Map<ColumnRef, List<ColumnAttributes>> allColumnAttributes = queryAllColumnAttributes(query);
        allColumnAttributes.forEach((columnRef, columnAttributes) -> {
            TableRef tableRef = columnRef.table;
            tables.putIfAbsent(tableRef, new TableDefinition(tableRef));
            TableDefinition tableDefinition = tables.computeIfAbsent(tableRef, TableDefinition::new);

            for (ColumnAttributes attributes : columnAttributes) {
                attributes.referencedColumn.ifPresent(referencedColumn -> {
                    ForeignKey fKey = tableDefinition.foreignKeys.getOrDefault(referencedColumn.table, new ForeignKey());
                    fKey.columns.add(columnRef.columnName);
                    fKey.referencedColumns.add(referencedColumn.columnName);
                    tableDefinition.foreignKeys.put(referencedColumn.table, fKey);
                });
            }

            ColumnDefinition columnDef = new ColumnDefinition(columnRef.columnName, columnAttributes.get(0).columnType, columnAttributes.get(0).primaryKey);

            if (tableDefinition.columns.contains(columnDef))
                return;
            tableDefinition.columns.add(columnDef);
        });
        return tables;
    }

    /**
     * Query the information_schema for the column attributes of all columns in all tables in all schemas
     * @param query an interface for querying a database
     * @return a list of attributes for each column
     */
    static Map<ColumnRef, List<ColumnAttributes>> queryAllColumnAttributes(Query query) {

        List<String> systemSchemasList = ImmutableList.of(
                "performance_schema",
                "information_schema",
                "mysql",
                "sys",
                "innodb");

        String systemSchemas = systemSchemasList.stream().map(s -> "'" + s + "'").collect(Collectors.joining(","));

        String selectTableColumnAttributes = "SELECT\n" +
                "    c.TABLE_SCHEMA,\n" +
                "    c.TABLE_NAME,\n" +
                "    c.COLUMN_NAME,\n" +
                "    c.ORDINAL_POSITION,\n" +
                "    c.COLUMN_TYPE,\n" +
                "    c.CHARACTER_SET_NAME,\n" +
                "    c.COLUMN_KEY,\n" +
                "    k.REFERENCED_TABLE_SCHEMA,\n" +
                "    k.REFERENCED_TABLE_NAME,\n" +
                "    k.REFERENCED_COLUMN_NAME\n" +
                "FROM information_schema.COLUMNS c\n" +
                "LEFT JOIN information_schema.KEY_COLUMN_USAGE k\n" +
                "    ON k.TABLE_SCHEMA = c.TABLE_SCHEMA COLLATE 'utf8_bin'\n" +
                "    AND k.TABLE_NAME = c.TABLE_NAME COLLATE 'utf8_bin'\n" +
                "    AND k.COLUMN_NAME = c.COLUMN_NAME COLLATE 'utf8_bin'\n" +
                "LEFT JOIN information_schema.TABLES t\n" +
                "    ON t.TABLE_SCHEMA = c.TABLE_SCHEMA COLLATE 'utf8_bin'\n" +
                "    AND t.TABLE_NAME = c.TABLE_NAME COLLATE 'utf8_bin'\n" +
                "WHERE t.TABLE_TYPE = 'BASE TABLE'\n" +
                "    AND c.TABLE_SCHEMA NOT IN (" + systemSchemas + ")\n" +
                "ORDER BY c.TABLE_SCHEMA COLLATE 'utf8_bin',\n" +
                "         c.TABLE_NAME COLLATE 'utf8_bin',\n" +
                "         c.ORDINAL_POSITION";

        List<Record> records = query.records(selectTableColumnAttributes);

        Map<ColumnRef, List<ColumnAttributes>> allColumnAttributes = new LinkedHashMap<>();
        for (Record record : records) {

            ColumnRef column = new ColumnRef(
                    new TableRef(record.get("TABLE_SCHEMA"), record.get("TABLE_NAME")),
                    record.get("COLUMN_NAME")
            );

            String referencedTableSchema = record.get("REFERENCED_TABLE_SCHEMA");
            String referencedTableName = record.get("REFERENCED_TABLE_NAME");
            String referencedColumnName = record.get("REFERENCED_COLUMN_NAME");
            Optional<ColumnRef> referencedColumn;
            if (referencedTableSchema != null && referencedTableName != null && referencedColumnName != null) {
                referencedColumn = Optional.of(new ColumnRef(
                        new TableRef(referencedTableSchema, referencedTableName),
                        referencedColumnName)
                );
            } else {
                referencedColumn = Optional.empty();
            }

            List<ColumnAttributes> columnAttributesList = allColumnAttributes.computeIfAbsent(column, c -> new ArrayList<>());
            columnAttributesList.add(new ColumnAttributes(
                    new Integer(record.get("ORDINAL_POSITION")),
                    record.get("COLUMN_TYPE"),
                    Optional.ofNullable(record.get("CHARACTER_SET_NAME")),
                    Objects.equals(record.get("COLUMN_KEY"), "PRI"),
                    referencedColumn
            ));
        }
        return allColumnAttributes;
    }
}
