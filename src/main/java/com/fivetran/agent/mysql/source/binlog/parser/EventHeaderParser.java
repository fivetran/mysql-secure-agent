package com.fivetran.agent.mysql.source.binlog.parser;

import com.fivetran.agent.mysql.source.binlog.BinlogInputStream;

import java.io.IOException;

import static com.fivetran.agent.mysql.source.binlog.client.BinlogClient.FOUR_BYTES;
import static com.fivetran.agent.mysql.source.binlog.client.BinlogClient.ONE_BYTE;
import static com.fivetran.agent.mysql.source.binlog.client.BinlogClient.TWO_BYTES;
import static com.fivetran.agent.mysql.source.binlog.parser.EventHeader.HEADER_LENGTH;

public class EventHeaderParser {

    public EventHeader parse(byte[] input) {
        if (input.length != HEADER_LENGTH)
            throw new RuntimeException("Improper event header length");

        BinlogInputStream in = new BinlogInputStream(input);
        EventHeader header = new EventHeader();

        header.setTimestamp(in.readInteger(FOUR_BYTES));
        header.setType(EventType.values()[in.readInteger(ONE_BYTE)]);
        header.setServerId(in.readInteger(FOUR_BYTES));
        header.setEventLength(in.readInteger(FOUR_BYTES));
        header.setNextPosition(in.readInteger(FOUR_BYTES));
        header.setFlags(in.readInteger(TWO_BYTES));

        return header;
    }
}
