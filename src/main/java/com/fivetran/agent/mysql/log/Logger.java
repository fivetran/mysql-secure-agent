package com.fivetran.agent.mysql.log;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fivetran.agent.mysql.Log;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.function.Consumer;

import static com.fivetran.agent.mysql.Main.JSON;


public class Logger implements Log {

    private Consumer<ByteBuffer> writer;

    public Logger(Consumer<ByteBuffer> writer) {
        this.writer = writer;
    }

    @Override
    public void log(LogMessage message) {
        try {
            ObjectNode logMessageJson = JSON.createObjectNode();
            ObjectNode messageContent = JSON.convertValue(message, ObjectNode.class);
            logMessageJson.put("level", String.valueOf(message.level()));
            logMessageJson.put("event", message.event().name());
            logMessageJson.put("timestamp", Instant.now().toString());
            if (!messageContent.isNull())
                logMessageJson.set("details", messageContent);
            String formattedLogMessage = JSON.writeValueAsString(logMessageJson) + '\n';
            writer.accept(ByteBuffer.wrap(formattedLogMessage.getBytes()));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
