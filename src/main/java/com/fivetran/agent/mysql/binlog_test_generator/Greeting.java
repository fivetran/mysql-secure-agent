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
package com.fivetran.agent.mysql.binlog_test_generator;

import java.io.ByteArrayInputStream;
import java.io.IOException;

public class Greeting {

    // todo: likely move this along with related classes to another repo once tests no longer need to be generated

    public int protocolVersion;
    public String serverVersion;
    public long threadId;
    public String scramble;
    public int serverCapabilities;
    public int serverCollation;
    public int serverStatus;
    public String pluginProvidedData;

    public Greeting(byte[] packet) throws IOException {
        try (ByteArrayInputStream buffer = new ByteArrayInputStream(packet)) {
            this.protocolVersion = PacketUtil.readInt(buffer, 1);
            this.serverVersion = PacketUtil.readString(buffer);
            this.threadId = PacketUtil.readLong(buffer, 4);
            String scramblePrefix = PacketUtil.readString(buffer);
            this.serverCapabilities = PacketUtil.readInt(buffer, 2);
            this.serverCollation = PacketUtil.readInt(buffer, 1);
            this.serverStatus = PacketUtil.readInt(buffer, 2);
            buffer.skip(13); // reserved
            this.scramble = scramblePrefix + PacketUtil.readString(buffer);
            if (buffer.available() > 0) {
                this.pluginProvidedData = PacketUtil.readString(buffer);
            }
        }
    }
}
