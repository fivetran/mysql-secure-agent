/**
* Copyright (c) Fivetran 2018
**/
package com.fivetran.agent.mysql.log;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.function.Consumer;

// TODO better name
public class RealLogWriter implements Consumer<ByteBuffer> {
    FileChannel channel;

    public RealLogWriter() {
        try {
            channel = FileChannel.open(Paths.get("agent-log-" + Instant.now().getEpochSecond() + ".json"), StandardOpenOption.WRITE, StandardOpenOption.CREATE);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void accept(ByteBuffer byteBuffer) {
        try {
            channel.write(byteBuffer);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
