/**
* Copyright (c) Fivetran 2018
**/
package com.fivetran.agent.mysql.output;

import java.util.Objects;
import java.util.Optional;

public class Emit {
    // TODO Emit.row just creates a new row, doesn't add it to the output - the name is misleading
    // TODO we don't need both fields coexisting in the same class
    public final Optional<Event> row;
    public final Optional<TableDefinition> tableDefinition;

    private Emit(Optional<Event> row, Optional<TableDefinition> tableDefinition) {
        this.row = row;
        this.tableDefinition = tableDefinition;
    }

    public static Emit row(Event inner) {
        return new Emit(Optional.of(inner), Optional.empty());
    }

    public static Emit tableDefinition(TableDefinition inner) {
        return new Emit(Optional.empty(), Optional.of(inner));
    }

    @Override
    public String toString() {
        return "Emit{" +
                "row=" + row +
                ", tableDefinition=" + tableDefinition +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Emit emit = (Emit) o;
        return Objects.equals(row, emit.row) &&
                Objects.equals(tableDefinition, emit.tableDefinition);
    }

    @Override
    public int hashCode() {
        return Objects.hash(row, tableDefinition);
    }
}
