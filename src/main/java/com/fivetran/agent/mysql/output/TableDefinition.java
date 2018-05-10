/**
* Copyright (c) Fivetran 2018
**/
package com.fivetran.agent.mysql.output;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fivetran.agent.mysql.source.TableRef;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class TableDefinition {
    @JsonProperty("table")
    public final TableRef table;
    @JsonProperty("tableDefinition")
    public final List<ColumnDefinition> columns;

    /** Multiple foreign keys can exist on a table in MySQL. This set represents
     * all of the foreign keys present on this table
     */
    public Map<TableRef, ForeignKey> foreignKeys;

    public TableDefinition(TableRef table, List<ColumnDefinition> columns, Map<TableRef, ForeignKey> foreignKeys) {
        this.table = table;
        this.columns = columns;
        this.foreignKeys = foreignKeys;
    }

    public TableDefinition(TableRef table, List<ColumnDefinition> columns) {
        this.table = table;
        this.columns = columns;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TableDefinition that = (TableDefinition) o;

        if (!table.equals(that.table)) return false;
        return columns.equals(that.columns);
    }

    @Override
    public int hashCode() {
        int result = table.hashCode();
        result = 31 * result + columns.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "TableDefinition{" +
                "table=" + table +
                ", columns=" + columns +
                '}';
    }
}
