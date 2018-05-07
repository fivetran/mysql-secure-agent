/**
* Copyright (c) Fivetran 2018
**/
package com.fivetran.agent.mysql.output;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fivetran.agent.mysql.source.TableRef;

import java.util.List;
import java.util.Optional;

import static com.fivetran.agent.mysql.output.Event.EventType.*;

public class Event {
    @JsonIgnore
    public final TableRef tableRef;
    @JsonIgnore
    public final EventType eventType;

    public final Optional<Upsert> upsert;
    public final Optional<Delete> delete;
    public final Optional<TableDefinition> tableDefinition;

    public enum EventType {
        UPSERT, DELETE, TABLE_DEFINITION, NOP
    }

    public static Event createNop() {
        return new Event();
    }

    public static Event createUpsert(TableRef tableRef, List<String> row) {
        return new Event(tableRef, new Upsert(tableRef, row));
    }

    public static Event createDelete(TableRef tableRef, List<String> row) {
        return new Event(tableRef, new Delete(tableRef, row));
    }

    public static Event createTableDefinition(TableDefinition tableDefinition) {
        return new Event(tableDefinition);
    }

    Event(TableRef tableRef, Upsert upsert) {
        this.tableRef = tableRef;
        this.tableDefinition = Optional.empty();
        this.upsert = Optional.of(upsert);
        this.delete = Optional.empty();
        this.eventType = UPSERT;
    }

    Event(TableRef tableRef, Delete delete) {
        this.tableRef = tableRef;
        this.tableDefinition = Optional.empty();
        this.upsert = Optional.empty();
        this.delete = Optional.of(delete);
        this.eventType = DELETE;
    }

    Event(TableDefinition tableDefinition) {
        this.tableRef = tableDefinition.table;
        this.tableDefinition = Optional.of(tableDefinition);
        this.upsert = Optional.empty();
        this.delete = Optional.empty();
        this.eventType = TABLE_DEFINITION;
    }

    Event() {
        this.tableRef = new TableRef("", "");
        this.tableDefinition = Optional.empty();
        this.upsert = Optional.empty();
        this.delete = Optional.empty();
        this.eventType = NOP;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Event event = (Event) o;

        if (!upsert.equals(event.upsert)) return false;
        if (!delete.equals(event.delete)) return false;
        return tableDefinition.equals(event.tableDefinition);
    }

    @Override
    public int hashCode() {
        int result = upsert.hashCode();
        result = 31 * result + delete.hashCode();
        result = 31 * result + tableDefinition.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "Event{" +
                "upsert=" + upsert +
                ", delete=" + delete +
                ", tableDefinition=" + tableDefinition +
                '}';
    }
}
