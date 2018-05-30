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

    private static final String RESOURCES_DIRECTORY = "resources";
    private static final String DATA_DIRECTORY = "data";
    static final String STATE_FILE = "mysql_state.json";
    static final String DATA_FILE_PREFIX = "mysql_data_";
    private static final String DATA_FILE_EXTENSION = ".json";
    private static final Duration AUTO_WRITE_INTERVAL = Duration.ofMinutes(15);
    private static final int MAX_OUTPUT_SIZE = 1024 * 1024 * 1024;
    private final int AUTO_WRITE_SIZE_LIMIT;
    private final BucketClient client;
    private Instant startTime;
    private FileChannel dataFileChannel;
    private Path path;
    private String checkpointState;

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

    private void writeToBuffer(Event event) throws IOException {
        String rowAsString = getRowAsString(event);
        dataFileChannel.write(ByteBuffer.wrap(rowAsString.getBytes()));
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
            default:
                throw new RuntimeException("Row was not associated with any event type");
        }
        return rowAsString + "\n";
    }

    private void checkpoint(AgentState state) {
        try {
            checkpointState = Serialize.value(state);

            Duration timeSinceWrite = Duration.between(startTime, Instant.now());
            if ((timeSinceWrite.compareTo(AUTO_WRITE_INTERVAL) > 0 && dataFileChannel.size() > 0)
                    || dataFileChannel.size() > AUTO_WRITE_SIZE_LIMIT) {
                flushToBucket();
            }
        } catch (IOException e) {
            LogMessage message = new LogGeneralException(e);
            LOG.log(message);
            throw new RuntimeException(e);
        }
    }

    private void flushToBucket() throws IOException {
        client.copy(DATA_DIRECTORY, path.toFile());
        dataFileChannel.close();
        refreshFileChannel();

        Path statePath = Paths.get(STATE_FILE);
        FileChannel stateFileChannel = FileChannel.open(statePath, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE);
        stateFileChannel.write(ByteBuffer.wrap(checkpointState.getBytes()));
        client.copy(RESOURCES_DIRECTORY, statePath.toFile());
        stateFileChannel.close();
    }

    private void refreshFileChannel() {
        path = Paths.get(DATA_FILE_PREFIX + Instant.now().getEpochSecond() + DATA_FILE_EXTENSION);
        try {
            dataFileChannel = FileChannel.open(path, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        startTime = Instant.now();
    }

    @Override
    public void close() {
        try {
            flushToBucket();
            dataFileChannel.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
