/**
* Copyright (c) Fivetran 2018
**/
package com.fivetran.agent.mysql.source.binlog.parser;

import com.fivetran.agent.mysql.source.TableRef;

public class TableMapEventBody implements EventBody {
    private TableRef table;
    private byte[] columnTypes;
    private int[] columnMetadata;
    private long tableId;

    public TableRef getTableRef() {
        return table;
    }

    public void setTableRef(TableRef table) {
        this.table = table;
    }

    public byte[] getColumnTypes() {
        return columnTypes;
    }

    public void setColumnTypes(byte[] columnTypes) {
        this.columnTypes = columnTypes;
    }

    public int[] getColumnMetadata() {
        return columnMetadata;
    }

    public void setColumnMetadata(int[] columnMetadata) {
        this.columnMetadata = columnMetadata;
    }

    public long getTableId() {
        return tableId;
    }

    public void setTableId(long tableId) {
        this.tableId = tableId;
    }
}
