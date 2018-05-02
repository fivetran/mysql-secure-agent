package com.fivetran.agent.mysql.log;

public enum LogEvent {
    BEGIN_READ_BINLOG,
    BEGIN_SELECT_PAGE,
    BINLOG_TIMEOUT,
    LOG_GENERAL_EXCEPTION
}
