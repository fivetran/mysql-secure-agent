/**
* Copyright (c) Fivetran 2018
**/
package com.fivetran.agent.mysql.log;

import com.fivetran.agent.mysql.source.TableRef;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class BeginSelectPage implements LogMessage {

    public BeginSelectPage(TableRef table) {
        this.table = table;
        this.fromKey = Optional.empty();
    }
    public BeginSelectPage(TableRef table, Optional<List<String>> fromKey) {
        this.table = table;
        this.fromKey = fromKey;
    }

    @Override
    public LogLevel level() {
        return LogLevel.INFO;
    }

    @Override
    public LogEvent event() {
        return LogEvent.BEGIN_SELECT_PAGE;
    }

    public final TableRef table;
    public final Optional<List<String>> fromKey;

    @Override
    public String toString() {
        return "BeginSelectPage{" +
                "table=" + table +
                ", fromKey=" + fromKey +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BeginSelectPage that = (BeginSelectPage) o;
        return Objects.equals(table.schemaName, that.table.schemaName) &&
                Objects.equals(table.tableName, that.table.tableName) &&
                Objects.equals(fromKey, that.fromKey);
    }

    @Override
    public int hashCode() {
        return Objects.hash(table.schemaName, table, fromKey);
    }
}
