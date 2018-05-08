/**
* Copyright (c) Fivetran 2018
**/
package com.fivetran.agent.mysql.source;

public enum SourceEventType {
    INSERT,
    UPDATE,
    DELETE,
    TIMEOUT,
    OTHER
}
