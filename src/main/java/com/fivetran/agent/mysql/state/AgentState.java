/**
* Copyright (c) Fivetran 2018
**/
package com.fivetran.agent.mysql.state;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fivetran.agent.mysql.deserialize.TableRefAsKeyDeserializer;
import com.fivetran.agent.mysql.output.TableDefinition;
import com.fivetran.agent.mysql.source.BinlogPosition;
import com.fivetran.agent.mysql.source.TableRef;

import java.util.*;

public class AgentState {
    public BinlogPosition binlogPosition;
    @JsonDeserialize(keyUsing = TableRefAsKeyDeserializer.class)
    public final Map<TableRef, TableState> tableStates = new HashMap<>();
    @JsonDeserialize(keyUsing = TableRefAsKeyDeserializer.class)
    public Map<TableRef, TableDefinition> tableDefinitions = new HashMap<>();

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        AgentState that = (AgentState) o;

        if (binlogPosition != null ? !binlogPosition.equals(that.binlogPosition) : that.binlogPosition != null)
            return false;
        return tableStates.equals(that.tableStates);
    }

    @Override
    public int hashCode() {
        int result = binlogPosition != null ? binlogPosition.hashCode() : 0;
        result = 31 * result + tableStates.hashCode();
        return result;
    }
}
