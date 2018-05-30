package com.fivetran.agent.mysql.output;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fivetran.agent.mysql.deserialize.TableRefAsKeyDeserializer;
import com.fivetran.agent.mysql.source.TableRef;

import java.util.HashMap;
import java.util.Map;

public class TableDefinitions {
    @JsonDeserialize(keyUsing = TableRefAsKeyDeserializer.class)
    public Map<TableRef, TableDefinition> tableDefinitions = new HashMap<>();

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TableDefinitions that = (TableDefinitions) o;

        return tableDefinitions != null ? tableDefinitions.equals(that.tableDefinitions) : that.tableDefinitions == null;
    }

    @Override
    public int hashCode() {
        return tableDefinitions != null ? tableDefinitions.hashCode() : 0;
    }
}
