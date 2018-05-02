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
}
