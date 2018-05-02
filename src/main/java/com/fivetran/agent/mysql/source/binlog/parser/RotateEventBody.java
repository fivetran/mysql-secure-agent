/**
* Copyright (c) Fivetran 2018
**/
package com.fivetran.agent.mysql.source.binlog.parser;

public class RotateEventBody implements EventBody {

    private String binlogFilename;
    private long binlogPosition;

    public String getBinlogFilename() {
        return binlogFilename;
    }

    public void setBinlogFilename(String binlogFilename) {
        this.binlogFilename = binlogFilename;
    }

    public long getBinlogPosition() {
        return binlogPosition;
    }

    public void setBinlogPosition(long binlogPosition) {
        this.binlogPosition = binlogPosition;
    }
}
