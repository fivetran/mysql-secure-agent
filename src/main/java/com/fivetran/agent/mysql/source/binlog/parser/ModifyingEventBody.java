/**
* Copyright (c) Fivetran 2018
**/
package com.fivetran.agent.mysql.source.binlog.parser;

import com.fivetran.agent.mysql.source.Row;
import com.fivetran.agent.mysql.source.TableRef;

import java.util.BitSet;
import java.util.List;

public class ModifyingEventBody implements EventBody {
    private TableRef table;
    private List<Row> oldRows;
    private List<Row> newRows;
    private BitSet includedColumns;
    private int columnCount;
    private long tableId;

    public TableRef getTableRef() {
        return table;
    }

    public void setTableRef(TableRef table) {
        this.table = table;
    }

    public List<Row> getNewRows() {
        return newRows;
    }

    public void setNewRows(List<Row> values) {
        this.newRows = values;
    }

    public int getColumnCount() {
        return columnCount;
    }

    public void setColumnCount(int columnCount) {
        this.columnCount = columnCount;
    }

    public BitSet getIncludedColumns() {
        return includedColumns;
    }

    public void setIncludedColumns(BitSet includedColumns) {
        this.includedColumns = includedColumns;
    }

    public long getTableId() {
        return tableId;
    }

    public void setTableId(long tableId) {
        this.tableId = tableId;
    }

    public List<Row> getOldRows() {
        return oldRows;
    }

    public void setOldRows(List<Row> oldRows) {
        this.oldRows = oldRows;
    }
}
