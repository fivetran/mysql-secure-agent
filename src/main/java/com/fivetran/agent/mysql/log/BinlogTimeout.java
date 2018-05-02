package com.fivetran.agent.mysql.log;

public class BinlogTimeout implements LogMessage {
    @Override
    public LogLevel level() {
        return LogLevel.INFO;
    }

    @Override
    public LogEvent event() {
        return LogEvent.BINLOG_TIMEOUT;
    }
}
