/**
* Copyright (c) Fivetran 2018
**/
package com.fivetran.agent.mysql.source;

public class MysqlUserException extends Exception {

    public final ServerConfig.UserError error;

    public MysqlUserException(ServerConfig.UserError error) {
        this.error = error;
    }

    @Override
    public String getMessage() {
        return error.longMessage;
    }
}
