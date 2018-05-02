/**
* Copyright (c) Fivetran 2018
**/
package com.fivetran.agent.mysql.log;

/**
 * All log messages must extend this interface.
 * Log messages will get converted into JSON like
 *   `{"level":"INFO", "event":"READ_BINLOG", "fromFile":"file1", "fromPosition":10}
 */
public interface LogMessage {
    LogLevel level();
    LogEvent event();
}
