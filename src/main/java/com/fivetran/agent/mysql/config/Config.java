/**
* Copyright (c) Fivetran 2018
**/
package com.fivetran.agent.mysql.config;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fivetran.agent.mysql.output.ColumnDefinition;
import com.fivetran.agent.mysql.output.TableDefinition;
import com.fivetran.agent.mysql.source.TableRef;

import java.util.*;

public class Config {
    public final Map<String, SchemaConfig> schemas = new HashMap<>();
    public boolean selectOtherSchemas = true;
    public String cryptoSalt = "";

    @JsonIgnore
    public Optional<SchemaConfig> getSchema(String schemaName) {
        return Optional.ofNullable(schemas.get(schemaName));
    }

    @JsonIgnore
    public Optional<TableConfig> getTable(TableRef tableRef) {
        if (!schemas.containsKey(tableRef.schemaName) ||
                !schemas.get(tableRef.schemaName).tables.containsKey(tableRef.tableName))
            return Optional.empty();
        return Optional.of(schemas.get(tableRef.schemaName).tables.get(tableRef.tableName));
    }

    @JsonIgnore
    public Optional<ColumnConfig> getColumn(TableRef tableRef, String columnName) {
        if (!getTable(tableRef).isPresent())
            return Optional.empty();
        return Optional.ofNullable(schemas.get(tableRef.schemaName).tables.get(tableRef.tableName).columns.get(columnName));
    }

    public void putColumn(Config config, TableRef tableRef, String columnName, boolean selected, boolean hash) {
        config.schemas.putIfAbsent(tableRef.schemaName, new SchemaConfig());
        config.schemas.get(tableRef.schemaName).tables.putIfAbsent(tableRef.tableName, new TableConfig());
        config.schemas.get(tableRef.schemaName).tables.get(tableRef.tableName).columns.putIfAbsent(columnName, new ColumnConfig(selected, hash, Optional.empty()));
    }

    public List<ColumnDefinition> getColumnsToSync(TableDefinition tableDef) {
        // we can assume here that we are to sync whatever tables make it into this method
        // if there is no table config, then sync all columns
        List<ColumnDefinition> columns = new ArrayList<>();
        TableConfig tableConfig = getTable(tableDef.table).orElse(new TableConfig());

        for (ColumnDefinition columnDef : tableDef.columns) {
            Optional<ColumnConfig> maybeColumnConfig = getColumn(tableDef.table, columnDef.name);
            ColumnConfig columnConfig;

            // if we're selecting other columns, that means that we can give it a default value
            // if we're not selecting other columns, we should not assign a default
            if (tableConfig.selectOtherColumns) {
                columnConfig = maybeColumnConfig.orElse(new ColumnConfig());
                if (columnConfig.selected)
                    columns.add(columnDef);
            } else if (maybeColumnConfig.isPresent() && maybeColumnConfig.get().selected) {
                columns.add(columnDef);
            }
        }
        return columns;
    }

    public  Map<TableRef, TableDefinition>getTablesToSync(Map<TableRef, TableDefinition> tableDefinitions) {
        Map<TableRef, TableDefinition> tablesToSync = new HashMap<>();
        for (TableDefinition tableDef : tableDefinitions.values()) {
            SchemaConfig schemaConfig = getSchema(tableDef.table.schemaName).orElse(new SchemaConfig());
            Optional<TableConfig> tableConfig = getTable(tableDef.table);
            if (ignorable(schemaConfig, tableConfig)) {
                continue;
            }
            tablesToSync.put(tableDef.table, tableDef);
        }
        return tablesToSync;
    }

    public static boolean ignorable(SchemaConfig schemaConfig, Optional<TableConfig> tableConfig) {
        return (tableConfig.isPresent() && !tableConfig.get().selected)
                || (!tableConfig.isPresent() && !schemaConfig.selectOtherTables);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Config config = (Config) o;

        if (selectOtherSchemas != config.selectOtherSchemas) return false;
        if (!schemas.equals(config.schemas)) return false;
        return cryptoSalt != null ? cryptoSalt.equals(config.cryptoSalt) : config.cryptoSalt == null;
    }

    @Override
    public int hashCode() {
        int result = schemas.hashCode();
        result = 31 * result + (selectOtherSchemas ? 1 : 0);
        result = 31 * result + (cryptoSalt != null ? cryptoSalt.hashCode() : 0);
        return result;
    }
}
