package com.fivetran.agent.mysql.source.binlog;

import com.fivetran.agent.mysql.source.BinlogPosition;
import com.fivetran.agent.mysql.source.binlog.parser.EventBody;
import com.fivetran.agent.mysql.source.binlog.parser.EventHeader;

public class BinlogEvent {

    private final EventHeader header;
    private final EventBody body;
    private final BinlogPosition currentPosition;

    public BinlogEvent(EventHeader header, EventBody body, BinlogPosition currentPosition) {
        this.header = header;
        this.body = body;
        this.currentPosition = currentPosition;
    }

    public EventHeader getHeader() {
        return header;
    }

    public EventBody getBody() {
        return body;
    }

    public BinlogPosition getCurrentPosition() {
        return currentPosition;
    }
}
