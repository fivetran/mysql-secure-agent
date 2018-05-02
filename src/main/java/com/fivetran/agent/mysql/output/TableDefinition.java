package com.fivetran.agent.mysql.output;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fivetran.agent.mysql.source.TableRef;

import java.util.List;

public class TableDefinition {
    @JsonProperty("table")
    public final TableRef table;
    @JsonProperty("tableDefinition")
    public final List<ColumnDefinition> columns;

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
