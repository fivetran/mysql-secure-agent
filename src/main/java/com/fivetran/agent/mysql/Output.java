package com.fivetran.agent.mysql;

import com.fivetran.agent.mysql.output.Event;
import com.fivetran.agent.mysql.state.AgentState;

public interface Output extends AutoCloseable {
    void emitEvent(Event event, AgentState state);
}
