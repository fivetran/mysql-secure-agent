/**
* Copyright (c) Fivetran 2018
**/
package com.fivetran.agent.mysql.output;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fivetran.agent.mysql.Output;
import com.fivetran.agent.mysql.Serialize;
import com.fivetran.agent.mysql.log.LogGeneralException;
import com.fivetran.agent.mysql.log.LogMessage;
import com.fivetran.agent.mysql.source.TableRef;
import com.fivetran.agent.mysql.state.AgentState;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import static com.fivetran.agent.mysql.Main.JSON;
import static com.fivetran.agent.mysql.Main.LOG;

public class BucketOutput implements Output {
    private static final Duration AUTO_WRITE_INTERVAL = Duration.ofMinutes(15);
    private static final int MAX_OUTPUT_SIZE = 1024 * 1024 * 1024;
    private final int AUTO_WRITE_SIZE_LIMIT;
    private final Map<TableRef, TableDefinition> tableDefinitions = new HashMap<>();
    private final BucketClient client;
    private Instant startTime;
    private FileChannel fileChannel;
    private Path path;

    public BucketOutput(BucketClient client) {
        this.client = client;
        this.AUTO_WRITE_SIZE_LIMIT = MAX_OUTPUT_SIZE;
        refreshFileChannel();
    }

    public BucketOutput(BucketClient client, int maxOutputSize) {
        this.AUTO_WRITE_SIZE_LIMIT = maxOutputSize;
        this.client = client;
        refreshFileChannel();
    }

    @Override
    public void emitEvent(Event event, AgentState state) {
        try {
            switch (event.eventType) {
                case DELETE:
                case UPSERT:
                    writeToBuffer(event);
                    break;
                case TABLE_DEFINITION:
                    processTableDefinitionEvent(event);
                    break;
                case NOP:
                    break;
                default:
                    throw new RuntimeException("Unexpected event type: " + event.eventType);
            }

            checkpoint(state);
        } catch (IOException e) {
            LogMessage message = new LogGeneralException(e);
            LOG.log(message);
            throw new RuntimeException(e);
        }
    }

    private void processTableDefinitionEvent(Event event) throws IOException {
        TableRef tableRef = event.tableRef;
        TableDefinition cachedTableDefinition = tableDefinitions.get(tableRef);
        TableDefinition eventTableDefinition = event.tableDefinition.orElseThrow(() ->
                new RuntimeException("TableDefinition instance must be present in TableDefinition event"));

        if (cachedTableDefinition == null || !cachedTableDefinition.equals(eventTableDefinition)) {
            tableDefinitions.put(tableRef, eventTableDefinition);
            writeToBuffer(event);
        }
    }

    private void writeToBuffer(Event event) throws IOException {
        String rowAsString = getRowAsString(event);
        fileChannel.write(ByteBuffer.wrap(rowAsString.getBytes()));
    }

    private String getRowAsString(Event event) throws JsonProcessingException {
        String rowAsString;
        switch (event.eventType) {
            case UPSERT:
                rowAsString = JSON.writeValueAsString(event.upsert.orElseThrow(() ->
                        new RuntimeException("Upsert object was not instantiated in Event class")));
                break;
            case DELETE:
                rowAsString = JSON.writeValueAsString(event.delete.orElseThrow(() ->
                        new RuntimeException("Delete object was not instantiated in Event class")));
                break;
            case TABLE_DEFINITION:
                rowAsString = JSON.writeValueAsString(event.tableDefinition.orElseThrow(() ->
                        new RuntimeException("TableDefinition object was not instantiated in Event class")));
                break;
            default:
                throw new RuntimeException("Row was not associated with any event type");
        }
        return rowAsString + "\n";
    }

    private void checkpoint(AgentState state) {
        try {
            Duration timeSinceWrite = Duration.between(startTime, Instant.now());
            if ((timeSinceWrite.compareTo(AUTO_WRITE_INTERVAL) > 0 && fileChannel.size() > 0)
                    || fileChannel.size() > AUTO_WRITE_SIZE_LIMIT) {
                flushToBucket(state);
            }
        } catch (IOException e) {
            LogMessage message = new LogGeneralException(e);
            LOG.log(message);
            throw new RuntimeException(e);
        }
    }

    private void flushToBucket(AgentState state) throws IOException {
        client.copy("data", path.toFile());
        fileChannel.close();
        refreshFileChannel();

        String stateJson = Serialize.state(state);
        Path statePath = Paths.get("state" + Instant.now().getEpochSecond() + ".json");
        FileChannel stateFileChannel = FileChannel.open(statePath, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
        stateFileChannel.write(ByteBuffer.wrap(stateJson.getBytes()));
        client.copy("resources", statePath.toFile());
        stateFileChannel.close();
    }

    private void refreshFileChannel() {
        path = Paths.get(Instant.now().getEpochSecond() + ".json");
        try {
            fileChannel = FileChannel.open(path, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        startTime = Instant.now();
    }

    @Override
    public void close() {
        try {
            fileChannel.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
