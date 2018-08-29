/**
* Copyright (c) Fivetran 2018
**/
package com.fivetran.agent.mysql.source;

import java.util.List;

public class SourceEvent {
    public final TableRef tableRef;
    public final BinlogPosition binlogPosition;
    public final SourceEventType event;
    public final List<Row> oldRows;
    public final List<Row> newRows;

    private SourceEvent(TableRef tableRef, BinlogPosition binlogPosition, SourceEventType event, List<Row> oldRows, List<Row> newRows) {
        this.tableRef = tableRef;
        this.binlogPosition = binlogPosition;
        this.event = event;
        this.oldRows = oldRows;
        this.newRows = newRows;
    }

    public static SourceEvent createInsert(TableRef tableRef, BinlogPosition latestPosition, List<Row> newRows) {
        return new SourceEvent(tableRef, latestPosition, SourceEventType.INSERT, null, newRows);
    }

    public static SourceEvent createDelete(TableRef tableRef, BinlogPosition latestPosition, List<Row> newRows) {
        return new SourceEvent(tableRef, latestPosition, SourceEventType.DELETE, null, newRows);
    }

    public static SourceEvent createUpdate(TableRef tableRef, BinlogPosition latestPosition, List<Row> oldRows, List<Row> newRows) {
        return new SourceEvent(tableRef, latestPosition, SourceEventType.UPDATE, oldRows, newRows);
    }

    public static SourceEvent createTimeout(BinlogPosition latestPosition) {
        return new SourceEvent(null, latestPosition, SourceEventType.TIMEOUT, null, null);
    }

    public static SourceEvent createOther(BinlogPosition latestPosition) {
        return new SourceEvent(null, latestPosition, SourceEventType.OTHER, null, null);
    }

    @Override
    public boolean equals(Object o){
        if (!(o instanceof SourceEvent)) {
            return false;
        }
        SourceEvent that = (SourceEvent) o;
        if (!this.tableRef.equals(that.tableRef)) return false;
        if (!this.binlogPosition.equals(that.binlogPosition)) return false;
        if (!this.event.equals(that.event)) return false;
        if (this.oldRows == null && that.oldRows != null) return false;
        if (this.oldRows != null && that.oldRows == null) return false;
        if (this.newRows == null && that.newRows != null) return false;
        if (this.newRows != null && that.newRows == null) return false;
        if (this.oldRows != null) {
            if (!this.oldRows.equals(that.oldRows)) return false;
        }
        if (this.newRows != null) {
            return this.newRows.equals(that.newRows);
        }
        return true;
    }

    @Override
    public String toString() {
        return "SourceEvent{" +
                "table=" + tableRef +
                ", binlogPosition=" + binlogPosition +
                ", eventType=" + event +
                ", oldRows=" + oldRows +
                ", newRows=" + newRows +
                "}";
    }
}
