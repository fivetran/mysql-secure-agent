package com.fivetran.agent.mysql.source;

public class MysqlConfigException extends Exception {

    public final ServerConfig.ConfigError error;

    public MysqlConfigException(ServerConfig.ConfigError error) {
        this.error = error;
    }

    @Override
    public String getMessage() {
        return error.longMessage;
    }
}
