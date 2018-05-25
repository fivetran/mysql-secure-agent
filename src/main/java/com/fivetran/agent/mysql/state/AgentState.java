/**
* Copyright (c) Fivetran 2018
**/
package com.fivetran.agent.mysql.state;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fivetran.agent.mysql.deserialize.TableRefAsKeyDeserializer;
import com.fivetran.agent.mysql.source.BinlogPosition;
import com.fivetran.agent.mysql.source.TableRef;

import java.util.HashMap;
import java.util.Map;

public class AgentState {
    public BinlogPosition binlogPosition;
    @JsonDeserialize(keyUsing = TableRefAsKeyDeserializer.class)
    public final Map<TableRef, TableState> tables = new HashMap<>();

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        AgentState that = (AgentState) o;

        if (binlogPosition != null ? !binlogPosition.equals(that.binlogPosition) : that.binlogPosition != null)
            return false;
        return tables.equals(that.tables);
    }

    @Override
    public int hashCode() {
        int result = binlogPosition != null ? binlogPosition.hashCode() : 0;
        result = 31 * result + tables.hashCode();
        return result;
    }
}
