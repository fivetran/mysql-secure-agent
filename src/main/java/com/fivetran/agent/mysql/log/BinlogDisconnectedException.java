package com.fivetran.agent.mysql.log;

import java.io.IOException;

public class BinlogDisconnectedException extends RuntimeException {
    public BinlogDisconnectedException(IOException message) {
        super(message);
    }
}
