/**
* Copyright (c) Fivetran 2018
**/
/*
 * Copyright 2013 Stanley Shyiko
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.fivetran.agent.mysql.source.binlog.client.shyiko;

import com.fivetran.agent.mysql.source.binlog.BinlogInputStream;

import java.io.IOException;

import static com.fivetran.agent.mysql.source.binlog.client.BinlogClient.FOUR_BYTES;
import static com.fivetran.agent.mysql.source.binlog.client.BinlogClient.ONE_BYTE;
import static com.fivetran.agent.mysql.source.binlog.client.BinlogClient.TWO_BYTES;

public class Greeting {

    public int protocolVersion;
    public String serverVersion;
    public long threadId;
    public String scramble;
    public int serverCapabilities;
    public int serverCollation;
    public int serverStatus;
    public String pluginProvidedData;

    public Greeting(byte[] packet) throws IOException {
        try (BinlogInputStream buffer = new BinlogInputStream(packet)) {
            this.protocolVersion = buffer.readInteger(ONE_BYTE);
            this.serverVersion = buffer.readZeroTerminatedString();
            this.threadId = buffer.readLong(FOUR_BYTES);
            String scramblePrefix = buffer.readZeroTerminatedString();
            this.serverCapabilities = buffer.readInteger(TWO_BYTES);
            this.serverCollation = buffer.readInteger(ONE_BYTE);
            this.serverStatus = buffer.readInteger(TWO_BYTES);
            buffer.skip(13); // reserved
            this.scramble = scramblePrefix + buffer.readZeroTerminatedString();
            if (buffer.available() > 0) {
                this.pluginProvidedData = buffer.readZeroTerminatedString();
            }
        }
    }
}

