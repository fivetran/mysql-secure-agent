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
package com.fivetran.agent.mysql.source.binlog.parser.shyiko;

import com.fivetran.agent.mysql.source.binlog.BinlogInputStream;
import com.fivetran.agent.mysql.source.binlog.parser.TableMapEventBody;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.BitSet;

import static com.fivetran.agent.mysql.source.binlog.client.BinlogClient.*;

public class RowParser {

    private static final int DIG_PER_DEC = 9;
    private static final int[] DIG_TO_BYTES = {0, 1, 1, 2, 2, 3, 3, 4, 4, 4};
    private static final int TWO_BYTE_BOUNDARY = 256;
    private static final int EXT_INFO_BITSLICE = 0x30;

    public static int bigEndianInteger(byte[] bytes, int offset, int length) {
        int result = 0;
        for (int i = offset; i < (offset + length); i++) {
            byte b = bytes[i];
            result = (result << 8) | (b >= 0 ? (int) b : (b + 256));
        }
        return result;
    }

    public String[] parseRow(BinlogInputStream in,
                             TableMapEventBody tableMapEvent,
                             BitSet includedColumns) throws IOException {
        if (tableMapEvent == null)
            throw new RuntimeException("No TableMapEventData has been found for table id:" + tableMapEvent.getTableId() +
                    ". Usually that means that you have started reading binary log 'within the logical event group'" +
                    " (e.g. from WRITE_ROWS and not proceeding TABLE_MAP");

        byte[] types = tableMapEvent.getColumnTypes();
        int[] metadata = tableMapEvent.getColumnMetadata();
        String[] result = new String[numberOfBitsSet(includedColumns)];
        BitSet nullColumns = in.readBitSet(result.length, true);

        for (int i = 0, numberOfSkippedColumns = 0; i < types.length; i++) {
            if (!includedColumns.get(i)) {
                numberOfSkippedColumns++;
                continue;
            }
            int index = i - numberOfSkippedColumns;
            if (!nullColumns.get(index)) {
                // mysql-5.6.24 sql/log_event.cc log_event_print_value (line 1980)
                int typeCode = getLowByteAsInt(types[i]);
                int meta = metadata[i];
                int length = 0;

                if (typeCode == ColumnType.STRING.getCode()) {
                    if (meta < TWO_BYTE_BOUNDARY)
                        length = meta;
                    else {  // metadata is more than one byte
                        int highByte = getHighByteAsInt(meta), lowByte = getLowByteAsInt(meta);

                        if (containsExtraInfo(highByte)) {
                            typeCode = extractStringType(highByte);
                            length = extractStringLength(lowByte, highByte);
                        } else {
                            // mysql-5.6.24 sql/rpl_utility.h enum_field_types (line 278)
                            if (highByte == ColumnType.ENUM.getCode() || highByte == ColumnType.SET.getCode())
                                typeCode = highByte;

                            length = lowByte;
                        }
                    }
                }
                result[index] = parseValue(ColumnType.byCode(typeCode), meta, length, in);
            }
        }
        return result;
    }

    private boolean containsExtraInfo(int highByte) {
        return (highByte & EXT_INFO_BITSLICE) != EXT_INFO_BITSLICE;
    }

    private int extractStringType(int highByte) {
        return highByte | EXT_INFO_BITSLICE;
    }

    private int extractStringLength(int lowByte, int highByte) {
        return lowByte | (((highByte & EXT_INFO_BITSLICE) ^ EXT_INFO_BITSLICE) << 4);
    }

    private int getLowByteAsInt(int bytes) {
        return bytes & 0xFF;
    }

    private int getHighByteAsInt(int bytes) {
        return bytes >> 8;
    }

    private String parseValue(ColumnType type, int meta, int length, BinlogInputStream inputStream)
            throws IOException {
        switch (type) {
            case BIT:
                return parseBit(meta, inputStream);
            case TINY:
                return parseTiny(inputStream);
            case SHORT:
                return parseShort(inputStream);
            case INT24:
                return parseInt24(inputStream);
            case LONG:
                return parseLong(inputStream);
            case LONGLONG:
                return parseLongLong(inputStream);
            case FLOAT:
                return parseFloat(inputStream);
            case DOUBLE:
                return parseDouble(inputStream);
            case NEWDECIMAL:
                return parseDecimal(meta, inputStream);
            case DATE:
                return parseDate(inputStream);
            case TIME:
                return parseTime(inputStream);
            case TIME_V2:
                return parseTimeV2(meta, inputStream);
            case TIMESTAMP:
                return parseTimestamp(inputStream);
            case TIMESTAMP_V2:
                return parseTimestampV2(meta, inputStream);
            case YEAR:
                return deserializeYear(inputStream);
            case DATETIME:
                return parseDatetime(inputStream);
            case DATETIME_V2:
                return parseDatetimeV2(meta, inputStream);
            case STRING: // CHAR or BINARY
                return parseString(length, inputStream);
            case VARCHAR:
            case VAR_STRING: // VARCHAR or VARBINARY
                return parseVarString(meta, inputStream);
            case BLOB:
                return parseBlob(meta, inputStream);
            case ENUM:
                return parseEnum(length, inputStream);
            case SET:
                return parseSet(length, inputStream);
            case JSON:
                return parseJson(meta, inputStream);
            default:
                throw new IOException("Unsupported type " + type);
        }
    }

    // todo: this should be removed since we apparently don't support the year type. Keeping for now to test client
    protected String deserializeYear(BinlogInputStream inputStream) {
        return String.valueOf(1900 + inputStream.readInteger(ONE_BYTE));
    }

    private String parseBit(int meta, BinlogInputStream inputStream) {
        int bitSetLength = (getHighByteAsInt(meta)) * 8 + (getLowByteAsInt(meta));
        return String.valueOf(inputStream.readBitSet(bitSetLength, false));
    }

    private String parseTiny(BinlogInputStream inputStream) {
        return String.valueOf((int) ((byte) inputStream.readInteger(ONE_BYTE)));
    }

    private String parseShort(BinlogInputStream inputStream) {
        return String.valueOf((int) ((short) inputStream.readInteger(TWO_BYTES)));
    }

    private String parseInt24(BinlogInputStream inputStream) {
        return String.valueOf((inputStream.readInteger(THREE_BYTES) << 8) >> 8);
    }

    private String parseLong(BinlogInputStream inputStream) {
        return String.valueOf(inputStream.readInteger(FOUR_BYTES));
    }

    private String parseLongLong(BinlogInputStream inputStream) {
        return String.valueOf(inputStream.readLong(EIGHT_BYTES));
    }

    private String parseFloat(BinlogInputStream inputStream) {
        return String.valueOf(Float.intBitsToFloat(inputStream.readInteger(FOUR_BYTES)));
    }

    private String parseDouble(BinlogInputStream inputStream) {
        return String.valueOf(Double.longBitsToDouble(inputStream.readLong(EIGHT_BYTES)));
    }

    private String parseDecimal(int meta, BinlogInputStream inputStream) {
        int precision = getLowByteAsInt(meta);
        int scale = getHighByteAsInt(meta);
        int x = precision - scale;
        int ipd = x / DIG_PER_DEC, fpd = scale / DIG_PER_DEC;

        int decimalLength = (ipd << 2) + DIG_TO_BYTES[x - ipd * DIG_PER_DEC] +
                (fpd << 2) + DIG_TO_BYTES[scale - fpd * DIG_PER_DEC];

        return String.valueOf(asBigDecimal(precision, scale, inputStream.read(decimalLength)));
    }

    private String parseDate(BinlogInputStream inputStream) {
        /*
            A three-byte integer packed as YYYY×16×32 + MM×32 + DD
        */
        int value = inputStream.readInteger(THREE_BYTES);
        int day = value % 32;

        value >>>= 5;
        int month = value % 16;
        int year = value >> 4;

        return String.format("%04d-%02d-%02d", year, month, day);
    }

    private String parseTime(BinlogInputStream inputStream) {
        /*
            A three-byte integer packed as DD×24×3600 + HH×3600 + MM×60 + SS
        */
        int value = inputStream.readInteger(THREE_BYTES);
        int hours = split(value, 3, 2);
        int minutes = split(value, 3, 1);
        int seconds = split(value, 3, 0);

        return String.format("%02d:%02d:%02d", hours, minutes, seconds);
    }

    private String parseTimeV2(int meta, BinlogInputStream inputStream) {
        /*
            (in big endian)

            1 bit sign (1= non-negative, 0= negative)
            1 bit unused (reserved for future extensions)
            10 bits hour (0-838)
            6 bits minute (0-59)
            6 bits second (0-59)

            (3 bytes in total)

            + fractional-seconds storage (size depends on meta)
        */
        long time = bigEndianLong(inputStream.read(THREE_BYTES), 0, 3);
        int fsp = parseFractionalSeconds(meta, inputStream);

        int hour = bitSlice(time, 2, 10, 24);
        int minute = bitSlice(time, 12, 6, 24);
        int second = bitSlice(time, 18, 6, 24);

        return String.format("%02d:%02d:%02d.%06d", hour, minute, second, fsp);
    }

    private String parseTimestamp(BinlogInputStream inputStream) {
        /*
             A four-byte integer representing seconds UTC since the epoch ('1970-01-01 00:00:00' UTC)
         */
        long timestamp = inputStream.readLong(FOUR_BYTES) * 1000;
        return String.valueOf(new java.sql.Timestamp(timestamp));
    }

    private String parseTimestampV2(int meta, BinlogInputStream inputStream) {
        /*
            Same as before 5.6.4, but big endian rather than little endian
         */
        long millis = bigEndianLong(inputStream.read(FOUR_BYTES), 0, 4);
        int fsp = parseFractionalSeconds(meta, inputStream);
        long timestamp = millis * 1000;

        String timestampAsString = new java.sql.Timestamp(timestamp).toString();

        return String.valueOf(
                cropEmptyDecimal(timestampAsString) + String.format("%06d", fsp));
    }

    private String cropEmptyDecimal(String timeStamp) {
        return timeStamp.substring(0, timeStamp.length() - 1);
    }

    private String parseDatetime(BinlogInputStream inputStream) {
        /*
            Eight bytes: A four-byte integer for date packed as YYYY×10000 + MM×100 + DD
                         and a four-byte integer for time packed as HH×10000 + MM×100 + SS
         */
        long value = inputStream.readLong(EIGHT_BYTES);
        int year = split(value, 6, 5);
        int month = split(value, 6, 4);
        int day = split(value, 6, 3);
        int hours = split(value, 6, 2);
        int minutes = split(value, 6, 1);
        int seconds = split(value, 6, 0);
        return String.format("%04d-%02d-%02d %02d:%02d:%02d", year, month, day, hours, minutes, seconds);
    }

    private String parseDatetimeV2(int meta, BinlogInputStream inputStream) {
        /*
            (in big endian)

            1 bit sign (1= non-negative, 0= negative)
            17 bits year*13+month (year 0-9999, month 0-12)
            5 bits day (0-31)
            5 bits hour (0-23)
            6 bits minute (0-59)
            6 bits second (0-59)

            (5 bytes in total)

            + fractional-seconds storage (size depends on meta)
        */
        long datetime = bigEndianLong(inputStream.read(FIVE_BYTES), 0, 5);
        int yearMonth = bitSlice(datetime, 1, 17, 40);
        int fsp = parseFractionalSeconds(meta, inputStream);

        int year = yearMonth / 13;
        int month = yearMonth % 13;
        int day = bitSlice(datetime, 18, 5, 40);
        int hour = bitSlice(datetime, 23, 5, 40);
        int minute = bitSlice(datetime, 28, 6, 40);
        int second = bitSlice(datetime, 34, 6, 40);

        return String.format("%04d-%02d-%02d %02d:%02d:%02d.%06d", year, month, day, hour, minute, second, fsp);
    }

    private String parseString(int length, BinlogInputStream inputStream) {
        // charset is not present in the binary log (meaning there is no way to distinguish between CHAR / BINARY)
        int stringLength = length < 256 ? inputStream.readInteger(ONE_BYTE) : inputStream.readInteger(TWO_BYTES);

        return inputStream.readString(stringLength);
    }

    private String parseVarString(int meta, BinlogInputStream inputStream) {
        int varcharLength = meta < 256 ? inputStream.readInteger(ONE_BYTE) : inputStream.readInteger(TWO_BYTES);

        return inputStream.readString(varcharLength);
    }

    private String parseBlob(int meta, BinlogInputStream inputStream) {
        int blobLength = inputStream.readInteger(meta);
        return inputStream.readString(blobLength);
    }

    private String parseEnum(int length, BinlogInputStream inputStream) {
        return String.valueOf(inputStream.readInteger(length));
    }

    private String parseSet(int length, BinlogInputStream inputStream) {
        return String.valueOf(inputStream.readLong(length));
    }

    private String parseJson(int meta, BinlogInputStream inputStream) throws IOException {
        int blobLength = inputStream.readInteger(meta);
        return JsonBinaryParser.parseAsString(inputStream.read(blobLength));
    }

    private int parseFractionalSeconds(int meta, BinlogInputStream inputStream) {
        int length = (meta + 1) / 2;
        if (length > 0) {
            int fraction = bigEndianInteger(inputStream.read(length), 0, length);
            return fraction * (int) Math.pow(100, 3 - length);
        }
        return 0;
    }

    private static int bitSlice(long value, int bitOffset, int numberOfBits, int payloadSize) {
        long result = value >> payloadSize - (bitOffset + numberOfBits);
        return (int) (result & ((1 << numberOfBits) - 1));
    }

    private static int numberOfBitsSet(BitSet bitSet) {
        int result = 0;

        for (int i = bitSet.nextSetBit(0); i >= 0; i = bitSet.nextSetBit(i + 1))
            result++;

        return result;
    }

    private static int split(long value, int length, int index) {
        int[] result = new int[length];
        int divider = 100;

        for (int i = 0; i < length - 1; i++) {
            result[i] = (int) (value % divider);
            value /= divider;
        }
        result[length - 1] = (int) value;

        return result[index];
    }

    /**
     * see mysql/strings/decimal.c
     */
    public static BigDecimal asBigDecimal(int precision, int scale, byte[] value) {
        boolean positive = (value[0] & 0x80) == 0x80;
        value[0] ^= 0x80;

        if (!positive) {
            for (int i = 0; i < value.length; i++)
                value[i] ^= 0xFF;
        }

        int x = precision - scale;
        int ipDigits = x / DIG_PER_DEC;
        int ipDigitsX = x - ipDigits * DIG_PER_DEC;
        int ipSize = (ipDigits << 2) + DIG_TO_BYTES[ipDigitsX];
        int offset = DIG_TO_BYTES[ipDigitsX];
        BigDecimal ip = offset > 0 ? BigDecimal.valueOf(bigEndianInteger(value, 0, offset)) : BigDecimal.ZERO;

        for (; offset < ipSize; offset += 4) {
            int i = bigEndianInteger(value, offset, 4);
            ip = ip.movePointRight(DIG_PER_DEC).add(BigDecimal.valueOf(i));
        }

        int shift = 0;
        BigDecimal fp = BigDecimal.ZERO;

        for (; shift + DIG_PER_DEC <= scale; shift += DIG_PER_DEC, offset += 4) {
            int i = bigEndianInteger(value, offset, 4);
            fp = fp.add(BigDecimal.valueOf(i).movePointLeft(shift + DIG_PER_DEC));
        }

        if (shift < scale) {
            int i = bigEndianInteger(value, offset, DIG_TO_BYTES[scale - shift]);
            fp = fp.add(BigDecimal.valueOf(i).movePointLeft(scale));
        }

        BigDecimal result = ip.add(fp);
        return positive ? result : result.negate();
    }

    private static long bigEndianLong(byte[] bytes, int offset, int length) {
        long result = 0;

        for (int i = offset; i < (offset + length); i++) {
            byte b = bytes[i];
            result = (result << 8) | (b >= 0 ? (int) b : (b + 256));
        }

        return result;
    }
}
