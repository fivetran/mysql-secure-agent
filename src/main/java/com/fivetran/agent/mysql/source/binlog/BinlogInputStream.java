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

package com.fivetran.agent.mysql.source.binlog;

import java.io.ByteArrayInputStream;
import java.util.BitSet;

import static com.fivetran.agent.mysql.source.binlog.client.BinlogClient.*;

/**
 * @author <a href="mailto:stanley.shyiko@gmail.com">Stanley Shyiko</a>
 */

public class BinlogInputStream extends ByteArrayInputStream {

    private static final int ONE_BYTE_PACKED_INTEGER = 250;
    private static final int NULL_PACKED_INTEGER = 251;
    private static final int TWO_BYTE_PACKED_INTEGER = 252;
    private static final int THREE_BYTE_PACKED_INTEGER = 253;
    private static final int EIGHT_BYTE_PACKED_INTEGER = 254;

    public BinlogInputStream(byte[] buf) {
        super(buf);
    }

    /**
     * Read int written in little-endian format.
     */
    public int readInteger(int length) {
        int result = 0;

        for (int i = 0; i < length; ++i)
            result |= (super.read() << (i << 3));

        return result;
    }

    /**
     * Read long written in little-endian format.
     */
    public long readLong(int length) {
        long result = 0;

        for (int i = 0; i < length; ++i)
            result |= (((long) super.read()) << (i << 3));

        return result;
    }

    /**
     * Read fixed length string.
     */
    public String readString(int length) {
        return new String(read(length));
    }

    /**
     * Read variable-length string. End is indicated by 0x00 byte.
     */
    public String readZeroTerminatedString() {
        StringBuilder sb = new StringBuilder();

        for (int b; (b = super.read()) != 0; )
            sb.append((char) (0x000000FF & b));

        return sb.toString();
    }

    public byte[] read(int length) {
        byte[] bytes = new byte[length];
        super.read(bytes, 0, length);

        return bytes;
    }

    public BitSet readBitSet(int length, boolean bigEndian) {
        // according to MySQL internals the amount of storage required for N columns is INT((N+7)/8) bytes
        byte[] bytes = read((length + 7) >> 3);
        BitSet result = new BitSet();

        bytes = bigEndian ? bytes : reverse(bytes);

        for (int i = 0; i < length; i++) {
            if ((bytes[i >> 3] & (1 << (i % 8))) != 0)
                result.set(i);
        }
        return result;
    }

    private byte[] reverse(byte[] bytes) {
        for (int i = 0, length = bytes.length >> 1; i < length; i++) {
            int j = bytes.length - 1 - i;
            byte t = bytes[i];

            bytes[i] = bytes[j];
            bytes[j] = t;
        }
        return bytes;
    }

    public void skipPackedInteger() {
        readPackedInteger();
    }

    public int readPackedInteger() {
        Number number = readPackedNumber();

        if (number == null)
            throw new RuntimeException("Unexpected NULL where int should have been");

        if (number.longValue() > Integer.MAX_VALUE)
            throw new RuntimeException("Stumbled upon long even though int expected");

        return number.intValue();
    }

    /**
     * Format (first-byte-based):<br>
     * 0-250 - The first byte is the number (in the range 0-250). No additional bytes are used.<br>
     * 251 - SQL NULL value<br>
     * 252 - Two more bytes are used. The number is in the range 251-0xffff.<br>
     * 253 - Three more bytes are used. The number is in the range 0xffff-0xffffff.<br>
     * 254 - Eight more bytes are used. The number is in the range 0xffffff-0xffffffffffffffff.
     */
    private Number readPackedNumber() {
        int b = super.read();

        if (b < ONE_BYTE_PACKED_INTEGER)
            return b;
        else if (b == NULL_PACKED_INTEGER)
            return null;
        else if (b == TWO_BYTE_PACKED_INTEGER)
            return (long) readInteger(TWO_BYTES);
        else if (b == THREE_BYTE_PACKED_INTEGER)
            return (long) readInteger(THREE_BYTES);
        else if (b == EIGHT_BYTE_PACKED_INTEGER)
            return readLong(EIGHT_BYTES);

        throw new RuntimeException("Unexpected packed number byte " + b);
    }

    public int readHeaderPacketLength() {
        int packetLength = readInteger(THREE_BYTES);
        super.skip(ONE_BYTE); // Sequence ID

        int marker = super.read();

        if (marker == 0xFF)
            return -1;

        return packetLength;
    }
}
