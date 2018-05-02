package com.fivetran.agent.mysql.log;

import java.util.concurrent.TimeoutException;

public class NoBinlogEventsReceived extends RuntimeException {
    public NoBinlogEventsReceived(TimeoutException message) {
        super(message);
    }
}
