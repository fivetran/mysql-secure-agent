package com.fivetran.agent.mysql.log;

public class LogGeneralException implements LogMessage {

    public final String message;

    public LogGeneralException(Throwable exception) {
        this.message = exception.getMessage();
    }

    @Override
    public LogLevel level() {
        return LogLevel.ERROR;
    }

    @Override
    public LogEvent event() {
        return LogEvent.LOG_GENERAL_EXCEPTION;
    }
}
