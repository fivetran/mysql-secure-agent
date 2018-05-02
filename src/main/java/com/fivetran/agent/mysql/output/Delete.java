package com.fivetran.agent.mysql.output;


import com.fasterxml.jackson.annotation.JsonProperty;
import com.fivetran.agent.mysql.source.TableRef;

import java.util.List;

public class Delete {
    @JsonProperty("table")
    public final TableRef table;
    @JsonProperty("delete")
    public final List<String> values;


    public Delete(TableRef tableRef, List<String> values) {
        this.table = tableRef;
        this.values = values;
    }

    @Override
    public String toString() {
        return "Delete{" +
                "table=" + table +
                ", values=" + values +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Delete that = (Delete) o;

        if (!table.equals(that.table)) return false;
        return values != null ? values.equals(that.values) : that.values == null;
    }

    @Override
    public int hashCode() {
        return values != null ? values.hashCode() : 0;
    }
}
