/**
* Copyright (c) Fivetran 2018
**/
package com.fivetran.agent.mysql.source;

import com.fivetran.agent.mysql.source.binlog.client.BinlogClient;

import java.util.Optional;

public interface ReadBinlog {
    /**
     * Connects to the binlog and returns an iterable of output
     */
    BinlogClient events(BinlogPosition startAt, Optional<BinlogPosition> endBefore);
}
