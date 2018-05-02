package com.fivetran.agent.mysql.binlog_test_generator;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class PacketUtil {

    // todo: likely move this along with related classes to another repo once tests no longer need to be generated

    private static final int ONE_BYTE_PACKED_INTEGER = 250;
    private static final int NULL_PACKED_INTEGER = 251;
    private static final int TWO__BYTE_PACKED_INTEGER = 252;
    private static final int THREE_BYTE_PACKED_INTEGER = 253;
    private static final int EIGHT_BYTE_PACKED_INTEGER = 254;

    static String readLengthEncodedString(InputStream buffer) throws IOException {
        return readString(buffer, readPackedInteger(buffer));
    }

    private static String readString(InputStream in, int length) throws IOException {
        byte[] buffer = new byte[length];
        if (length > 0) {
            int numBytesRead = in.read(buffer);
            if (numBytesRead != length)
                throw new RuntimeException("Bytes read does not match expected payload length");
        }
        return new String(buffer);
    }

    static String readString(InputStream buffer) throws IOException {
        ByteArrayOutputStream s = new ByteArrayOutputStream();
        while (true) {
            int val = buffer.read();
            if (val != 0) s.write(val);
            else break;
        }
        return new String(s.toByteArray());
    }

    private static int readPackedInteger(InputStream buffer) throws IOException {
        Number number = readPackedNumber(buffer);
        if (number == null) {
            throw new IOException("Unexpected NULL where int should have been");
        }
        if (number.longValue() > Integer.MAX_VALUE) {
            throw new IOException("Stumbled upon long even though int expected");
        }
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
    private static Number readPackedNumber(InputStream buffer) throws IOException {
        int b = buffer.read();
        if (b < ONE_BYTE_PACKED_INTEGER) {
            return b;
        } else if (b == NULL_PACKED_INTEGER) {
            return null;
        } else if (b == TWO__BYTE_PACKED_INTEGER) {
            return (long) readInt(buffer, 2);
        } else if (b == THREE_BYTE_PACKED_INTEGER) {
            return (long) readInt(buffer, 3);
        } else if (b == EIGHT_BYTE_PACKED_INTEGER) {
            return readLong(buffer, 8);
        }
        throw new IOException("Unexpected packed number byte " + b);
    }

    static int readInt(InputStream buffer, int length) throws IOException {
        int result = 0;
        for (int i = 0; i < length; ++i) {
            result |= (buffer.read() << (i << 3));
        }
        return result;
    }

    static long readLong(InputStream buffer, int length) throws IOException {
        long result = 0;
        for (int i = 0; i < length; ++i) {
            result |= (((long) buffer.read()) << (i << 3));
        }
        return result;
    }

    static void writeString(OutputStream buffer, String value) throws IOException {
        buffer.write(value.getBytes());
        buffer.write(0);
    }

    static void writeInt(OutputStream buffer, int value, int length) throws IOException {
        for (int i = 0; i < length; i++) {
            buffer.write(0x000000FF & (value >>> (i << 3)));
        }
    }

    static void writeLong(OutputStream buffer, long value, int length) throws IOException {
        for (int i = 0; i < length; i++) {
            buffer.write((int) (0x00000000000000FF & (value >>> (i << 3))));
        }
    }
}
