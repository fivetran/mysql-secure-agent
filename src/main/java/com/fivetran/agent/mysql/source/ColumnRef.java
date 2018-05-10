/**
* Copyright (c) Fivetran 2018
**/
package com.fivetran.agent.mysql.source;

public class ColumnRef {
    public final TableRef table;
    public final String columnName;

    public ColumnRef(TableRef table, String columnName) {
        this.table = table;
        this.columnName = columnName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ColumnRef columnRef = (ColumnRef) o;

        if (table != null ? !table.equals(columnRef.table) : columnRef.table != null) return false;
        return columnName != null ? columnName.equals(columnRef.columnName) : columnRef.columnName == null;
    }

    @Override
    public int hashCode() {
        int result = table != null ? table.hashCode() : 0;
        result = 31 * result + (columnName != null ? columnName.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "ColumnRef{" +
                "table=" + table +
                ", columnName='" + columnName + '\'' +
                '}';
    }
}
