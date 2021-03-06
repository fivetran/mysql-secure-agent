/**
* Copyright (c) Fivetran 2018
**/
package com.fivetran.agent.mysql.output;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fivetran.agent.mysql.source.TableRef;

import java.util.List;

public class Upsert {
    @JsonProperty("table")
    public final TableRef table;
    @JsonProperty("upsert")
    public final List<String> values;

    public Upsert(TableRef table, List<String> values) {
        this.table = table;
        this.values = values;
    }

    @Override
    public String toString() {
        return "Upsert{" +
                "table=" + table +
                ", values=" + values +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Upsert that = (Upsert) o;

        if (!table.equals(that.table)) return false;
        return values != null ? values.equals(that.values) : that.values == null;
    }

    @Override
    public int hashCode() {
        return values != null ? values.hashCode() : 0;
    }
}
