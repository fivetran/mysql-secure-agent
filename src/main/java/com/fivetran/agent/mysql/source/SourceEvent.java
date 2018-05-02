package com.fivetran.agent.mysql.source;

import java.util.List;

public class SourceEvent {
    public final TableRef tableRef;
    public final BinlogPosition binlogPosition;
    public final SourceEventType event;
    public final List<Row> oldRows;
    public final List<Row> newRows;

    public SourceEvent(TableRef tableRef, BinlogPosition binlogPosition, SourceEventType event, List<Row> oldRow, List<Row> newRow) {
        this.tableRef = tableRef;
        this.binlogPosition = binlogPosition;
        this.event = event;
        this.oldRows = oldRow;
        this.newRows = newRow;
    }

    public SourceEvent(TableRef tableRef, BinlogPosition binlogPosition, SourceEventType event, List<Row> newRow) {
        this(tableRef, binlogPosition, event, null, newRow);
    }

    public static SourceEvent createTimeout(BinlogPosition latestPosition) {
        return new SourceEvent(null, latestPosition, SourceEventType.TIMEOUT, null, null);
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
        if (this.oldRows != null && that.oldRows != null) {
            if (!this.oldRows.equals(that.oldRows)) return false;
        }
        if (this.newRows != null && that.newRows != null) {
            if (!this.newRows.equals(that.newRows)) return false;
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
