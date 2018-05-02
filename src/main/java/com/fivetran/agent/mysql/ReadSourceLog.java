package com.fivetran.agent.mysql;

import com.fivetran.agent.mysql.source.BinlogPosition;
import com.fivetran.agent.mysql.source.binlog.client.EventReader;

public interface ReadSourceLog {

    /**
     * Gets the "head" of the binlog
     */
    BinlogPosition currentPosition();

    /**
     * From a given binlog position, returns all subsequent output in a common format
     * @param position
     * @return
     */
    EventReader events(BinlogPosition position);
}
