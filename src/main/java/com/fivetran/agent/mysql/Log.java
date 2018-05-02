/**
* Copyright (c) Fivetran 2018
**/
package com.fivetran.agent.mysql;

import com.fivetran.agent.mysql.log.LogMessage;

@FunctionalInterface
public interface Log {
    void log(LogMessage message);
}
