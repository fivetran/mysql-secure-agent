package com.fivetran.agent.mysql.log;

public class BeginReadBinlog implements LogMessage {
    @Override
    public LogLevel level() {
        return LogLevel.INFO;
    }

    @Override
    public LogEvent event() {
        return LogEvent.BEGIN_READ_BINLOG;
    }
}
