package com.fivetran.agent.mysql.output;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fivetran.agent.mysql.deserialize.TableRefDeserializer;
import com.fivetran.agent.mysql.source.TableRef;

import java.util.Objects;

public class BeginTable {
    @JsonProperty("begin_table")
    public final TableRef table;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BeginTable that = (BeginTable) o;
        return Objects.equals(table, that.table);
    }

    @Override
    public int hashCode() {
        return Objects.hash(table);
    }

    BeginTable(@JsonProperty("begin_table") @JsonDeserialize(using = TableRefDeserializer.class) TableRef beginTable) {
        this.table = beginTable;
    }
}
