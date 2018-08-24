/**
* Copyright (c) Fivetran 2018
**/
package com.fivetran.agent.mysql.output;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fivetran.agent.mysql.source.TableRef;

import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.fivetran.agent.mysql.output.Event.EventType.*;

public class Event {
    @JsonIgnore
    public final TableRef tableRef;
    @JsonIgnore
    public final EventType eventType;

    public final Optional<Upsert> upsert;
    public final Optional<Delete> delete;
    public final Optional<BeginTable> beginTable;

    public static Event createBeginTable(TableRef tableRef) {
        return new Event(new BeginTable(tableRef, Instant.now()));
    }

    public enum EventType {
        BEGIN_TABLE, UPSERT, DELETE, NOP
    }

    public static Event createNop() {
        return new Event();
    }

    public static Event createUpsert(TableRef tableRef, List<String> row) {
        return new Event(new Upsert(tableRef, row));
    }

    public static Event createDelete(TableRef tableRef, List<String> row) {
        return new Event(new Delete(tableRef, row));
    }

    private Event(BeginTable beginTable) {
        this.tableRef = beginTable.table;
        this.beginTable = Optional.of(beginTable);
        this.upsert = Optional.empty();
        this.delete = Optional.empty();
        this.eventType = BEGIN_TABLE;
    }

    private Event(Upsert upsert) {
        this.tableRef = upsert.table;
        this.beginTable = Optional.empty();
        this.upsert = Optional.of(upsert);
        this.delete = Optional.empty();
        this.eventType = UPSERT;
    }

    private Event(Delete delete) {
        this.tableRef = delete.table;
        this.beginTable = Optional.empty();
        this.upsert = Optional.empty();
        this.delete = Optional.of(delete);
        this.eventType = DELETE;
    }

    private Event() {
        this.tableRef = new TableRef("", "");
        this.beginTable = Optional.empty();
        this.upsert = Optional.empty();
        this.delete = Optional.empty();
        this.eventType = NOP;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Event event = (Event) o;
        return Objects.equals(tableRef, event.tableRef) &&
                eventType == event.eventType &&
                Objects.equals(upsert, event.upsert) &&
                Objects.equals(delete, event.delete);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableRef, eventType, upsert, delete);
    }

    @Override
    public String toString() {
        return "Event{" +
                "tableRef=" + tableRef +
                ", eventType=" + eventType +
                ", upsert=" + upsert +
                ", delete=" + delete +
                '}';
    }
}
