/**
* Copyright (c) Fivetran 2018
**/
package com.fivetran.agent.mysql.source.binlog.parser;

public class EventHeader {

    public static final int HEADER_LENGTH = 19;

    private int timestamp;
    private EventType type;
    private int serverId;
    private int eventLength;
    private int nextPosition;
    private int flags;

    public int getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(int timestamp) {
        this.timestamp = timestamp;
    }

    public EventType getType() {
        return type;
    }

    public void setType(EventType type) {
        this.type = type;
    }

    public int getServerId() {
        return serverId;
    }

    public void setServerId(int serverId) {
        this.serverId = serverId;
    }

    public int getEventLength() {
        return eventLength;
    }

    public void setEventLength(int eventLength) {
        this.eventLength = eventLength;
    }

    public int getNextPosition() {
        return nextPosition;
    }

    public void setNextPosition(int nextPosition) {
        this.nextPosition = nextPosition;
    }

    public int getFlags() {
        return flags;
    }

    public void setFlags(int flags) {
        this.flags = flags;
    }

    public int getBodyLength() {
        return eventLength - HEADER_LENGTH;
    }

    public int getHeaderLength() {
        return HEADER_LENGTH;
    }
}
