package com.fivetran.agent.mysql.source.binlog.client;

public class PacketReadException extends RuntimeException {
    public PacketReadException(String errorMessage) {
        super(errorMessage);
    }
}
