package com.fivetran.agent.mysql.source.binlog.parser;

import com.fivetran.agent.mysql.source.BinlogPosition;
import com.fivetran.agent.mysql.source.ReadBinlog;
import com.fivetran.agent.mysql.source.binlog.client.BinlogClient;

import java.util.Optional;

public class ReadMysqlBinlog implements ReadBinlog {

    @Override
    public BinlogClient events(BinlogPosition startAt, Optional<BinlogPosition> endBefore) {
        return null;
    }
}
