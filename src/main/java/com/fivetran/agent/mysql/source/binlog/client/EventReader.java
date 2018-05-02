/**
* Copyright (c) Fivetran 2018
**/
package com.fivetran.agent.mysql.source.binlog.client;

import com.fivetran.agent.mysql.source.SourceEvent;

public interface EventReader extends AutoCloseable {
    /**
     * Reads a single event from the binary log, blocking for 1 second if necessary.
     * Returns an Optional of SourceEvent if event is found. Otherwise, returns empty
     * @return
     */
    SourceEvent readEvent();
}
