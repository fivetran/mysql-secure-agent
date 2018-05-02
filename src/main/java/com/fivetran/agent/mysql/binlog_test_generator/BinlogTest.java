/**
* Copyright (c) Fivetran 2018
**/
package com.fivetran.agent.mysql.binlog_test_generator;

import com.amazonaws.util.IOUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.LinkedList;
import java.util.Optional;

import static com.fivetran.agent.mysql.binlog_test_generator.BinlogTest.BinlogTestTypes.*;

public class BinlogTest {
    private BinlogTestTypes eventType;
    private String column;
    private String type;
    private String value;
    private Optional<String> updateValue;
    private Optional<String> charSet;

    public enum BinlogTestTypes {
        WRITE, UPDATE, DELETE, TABLEMAP, HEADER, MASSIVE, FULL_EVENT
    }

    private BinlogTest(
            BinlogTestTypes eventType,
            String column,
            String type,
            String value,
            Optional<String> updateValue,
            Optional<String> charSet) {
        this.eventType = eventType;
        this.column = column;
        this.type = type;
        this.value = value;
        this.updateValue = updateValue;
        this.charSet = charSet;
    }

    public static LinkedList<BinlogTest> generateTests() {
        try {
            String massiveString = IOUtils.toString(new FileInputStream(new File("src/test/test_resources/sixteen-mb-string.txt")));

            return new LinkedList<BinlogTest>() {
                {
//                    add(new BinlogTest(FULL_EVENT, "fullEvent", "INT", "2147483647", Optional.empty(), Optional.empty()));
//                    add(new BinlogTest(WRITE, "massiveString", "LONGTEXT", massiveString, Optional.empty(), Optional.empty()));
//                    add(new BinlogTest(HEADER, "readHeader", "INT", "2147483647", Optional.empty(), Optional.empty()));
                    // todo: tableMap tests will need to be fixed
//                    add(new BinlogTest(TABLEMAP, "readTableMap", "INT", "2147483647", Optional.empty(), Optional.empty()));
//                    add(new BinlogTest(WRITE, "insertRow", "JSON", "'[1, \"a\"]'", Optional.empty(), Optional.empty()));
//                    add(new BinlogTest(UPDATE, "updateRow", "VARCHAR(25)", "'foobarbazqux'", Optional.of("'xuqzabraboof'"), Optional.empty()));
//                    add(new BinlogTest(DELETE, "deleteRow", "MEDIUMINT", "8388607", Optional.empty(), Optional.empty()));
//                    add(new BinlogTest(WRITE, "multiByte", "VARCHAR(25)", "'aÃbڅcㅙdソe'", Optional.empty(), Optional.empty()));

//                    add(new BinlogTest(WRITE, "big5Charset", "CHAR(1)", "'什'", Optional.empty(), Optional.of("CHARACTER SET big5")));
//                    add(new BinlogTest(WRITE, "cp850Charset", "CHAR(1)", "'Ç'", Optional.empty(), Optional.of("CHARACTER SET cp850")));
//                    add(new BinlogTest(WRITE, "koi8rCharset", "CHAR(1)", "'ж'", Optional.empty(), Optional.of("CHARACTER SET koi8r")));
//                    add(new BinlogTest(WRITE, "utf8Charset", "CHAR(1)", "'≈'", Optional.empty(), Optional.of("CHARACTER SET utf8")));
//                    add(new BinlogTest(WRITE, "latin1Charset", "CHAR(1)", "'Ð'", Optional.empty(), Optional.of("CHARACTER SET latin1")));
//                    add(new BinlogTest(WRITE, "latin2Charset", "CHAR(1)", "'ö'", Optional.empty(), Optional.of("CHARACTER SET latin2")));
//                    add(new BinlogTest(WRITE, "asciiCharset", "CHAR(1)", "'" + '\32' + "'", Optional.empty(), Optional.of("CHARACTER SET ascii")));
//                    add(new BinlogTest(WRITE, "ujisCharset", "CHAR(1)", "'ｹ'", Optional.empty(), Optional.of("CHARACTER SET ujis")));
//                    add(new BinlogTest(WRITE, "sjisCharset", "CHAR(1)", "'ボ'", Optional.empty(), Optional.of("CHARACTER SET sjis")));
//                    add(new BinlogTest(WRITE, "hebrewCharset", "CHAR(1)", "'ה'", Optional.empty(), Optional.of("CHARACTER SET hebrew")));
//                    add(new BinlogTest(WRITE, "tis620Charset", "CHAR(1)", "'ฬ'", Optional.empty(), Optional.of("CHARACTER SET tis620")));
//                    add(new BinlogTest(WRITE, "euckrCharset", "CHAR(1)", "'ㅝ'", Optional.empty(), Optional.of("CHARACTER SET euckr")));
//                    add(new BinlogTest(WRITE, "koi8uCharset", "CHAR(1)", "'й'", Optional.empty(), Optional.of("CHARACTER SET koi8u")));
//                    add(new BinlogTest(WRITE, "gb2312Charset", "CHAR(1)", "'与'", Optional.empty(), Optional.of("CHARACTER SET gb2312")));
//                    add(new BinlogTest(WRITE, "greekCharset", "CHAR(1)", "'ζ'", Optional.empty(), Optional.of("CHARACTER SET greek")));
//                    add(new BinlogTest(WRITE, "cp1250Charset", "CHAR(1)", "'ß'", Optional.empty(), Optional.of("CHARACTER SET cp1250")));
//                    add(new BinlogTest(WRITE, "gbkCharset", "CHAR(1)", "'堃'", Optional.empty(), Optional.of("CHARACTER SET gbk")));
//                    add(new BinlogTest(WRITE, "latin5Charset", "CHAR(1)", "'Æ'", Optional.empty(), Optional.of("CHARACTER SET latin5")));
//                    add(new BinlogTest(WRITE, "ucs2Charset", "CHAR(1)", "'Ը'", Optional.empty(), Optional.of("CHARACTER SET ucs2")));
//                    add(new BinlogTest(WRITE, "cp866Charset", "CHAR(1)", "'Є'", Optional.empty(), Optional.of("CHARACTER SET cp866")));
//                    add(new BinlogTest(WRITE, "macceCharset", "CHAR(1)", "'◊'", Optional.empty(), Optional.of("CHARACTER SET macce")));
//                    add(new BinlogTest(WRITE, "macromanCharset", "CHAR(1)", "'€'", Optional.empty(), Optional.of("CHARACTER SET macroman")));
//                    add(new BinlogTest(WRITE, "cp852Charset", "CHAR(1)", "'š'", Optional.empty(), Optional.of("CHARACTER SET cp852")));
//                    add(new BinlogTest(WRITE, "latin7Charset", "CHAR(1)", "'Ų'", Optional.empty(), Optional.of("CHARACTER SET latin7")));
//                    add(new BinlogTest(WRITE, "utf8mb4Charset", "CHAR(1)", "'Þ'", Optional.empty(), Optional.of("CHARACTER SET utf8mb4")));
//                    add(new BinlogTest(WRITE, "cp1251Charset", "CHAR(1)", "'¶'", Optional.empty(), Optional.of("CHARACTER SET cp1251")));
//                    add(new BinlogTest(WRITE, "utf16Charset", "CHAR(1)", "'Ȑ'", Optional.empty(), Optional.of("CHARACTER SET utf16")));
//                    add(new BinlogTest(WRITE, "utf16leCharset", "CHAR(1)", "'ȸ'", Optional.empty(), Optional.of("CHARACTER SET utf16le")));
//                    add(new BinlogTest(WRITE, "cp1256Charset", "CHAR(1)", "'ش'", Optional.empty(), Optional.of("CHARACTER SET cp1256")));
//                    add(new BinlogTest(WRITE, "cp1257Charset", "CHAR(1)", "'å'", Optional.empty(), Optional.of("CHARACTER SET cp1257")));
//                    add(new BinlogTest(WRITE, "utf32Charset", "CHAR(1)", "'ʆ'", Optional.empty(), Optional.of("CHARACTER SET utf32")));
//                    add(new BinlogTest(WRITE, "cp932Charset", "CHAR(1)", "'ﾂ'", Optional.empty(), Optional.of("CHARACTER SET cp932")));
//                    add(new BinlogTest(WRITE, "gb18030Charset", "CHAR(1)", "'Θ'", Optional.empty(), Optional.of("CHARACTER SET gb18030")));
//
//                    add(new BinlogTest(WRITE, "singleChar", "CHAR(1)", "'a'", Optional.empty(), Optional.empty()));
//                    add(new BinlogTest(WRITE, "multiChar", "CHAR(25)", "'foobarbazqux'", Optional.empty(), Optional.empty()));
//                    add(new BinlogTest(WRITE, "emptyChar", "CHAR(1)", "''", Optional.empty(), Optional.empty()));
//                    add(new BinlogTest(WRITE, "nullChar", "CHAR(1)", "null", Optional.empty(), Optional.empty()));
//
//                    add(new BinlogTest(WRITE, "singleVarchar", "VARCHAR(1)", "'a'", Optional.empty(), Optional.empty()));
//                    add(new BinlogTest(WRITE, "multiVarchar", "VARCHAR(25)", "'foobarbazqux'", Optional.empty(), Optional.empty()));
//                    add(new BinlogTest(WRITE, "emptyVarchar", "VARCHAR(1)", "''", Optional.empty(), Optional.empty()));
//                    add(new BinlogTest(WRITE, "nullVarchar", "VARCHAR(1)", "null", Optional.empty(), Optional.empty()));
//
//                    add(new BinlogTest(WRITE, "singleTinytext", "TINYTEXT", "'a'", Optional.empty(), Optional.empty()));
//                    add(new BinlogTest(WRITE, "multiTinytext", "TINYTEXT", "'foobarbazqux'", Optional.empty(), Optional.empty()));
//                    add(new BinlogTest(WRITE, "emptyTinytext", "TINYTEXT", "''", Optional.empty(), Optional.empty()));
//                    add(new BinlogTest(WRITE, "nullTinytext", "TINYTEXT", "null", Optional.empty(), Optional.empty()));
//
//                    add(new BinlogTest(WRITE, "singleMediumtext", "MEDIUMTEXT", "'a'", Optional.empty(), Optional.empty()));
//                    add(new BinlogTest(WRITE, "multiMediumtext", "MEDIUMTEXT", "'foobarbazqux'", Optional.empty(), Optional.empty()));
//                    add(new BinlogTest(WRITE, "emptyMediumtext", "MEDIUMTEXT", "''", Optional.empty(), Optional.empty()));
//                    add(new BinlogTest(WRITE, "nullMediumtext", "MEDIUMTEXT", "null", Optional.empty(), Optional.empty()));
//
//                    add(new BinlogTest(WRITE, "singleLongtext", "LONGTEXT", "'a'", Optional.empty(), Optional.empty()));
//                    add(new BinlogTest(WRITE, "multiLongtext", "LONGTEXT", "'foobarbazqux'", Optional.empty(), Optional.empty()));
//                    add(new BinlogTest(WRITE, "emptyLongtext", "LONGTEXT", "''", Optional.empty(), Optional.empty()));
//                    add(new BinlogTest(WRITE, "nullLongtext", "LONGTEXT", "null", Optional.empty(), Optional.empty()));
//
//                    add(new BinlogTest(WRITE, "singleEnum", "ENUM('a', 'b')", "'a'", Optional.empty(), Optional.empty()));
//                    add(new BinlogTest(WRITE, "multiEmum", "ENUM('foobar', 'bazqux')", "'foobar'", Optional.empty(), Optional.empty()));
//                    add(new BinlogTest(WRITE, "emptyEmum", "ENUM('', 'bazqux')", "''", Optional.empty(), Optional.empty()));
//                    add(new BinlogTest(WRITE, "nullEnum", "ENUM('foobar', 'bazqux')", "null", Optional.empty(), Optional.empty()));
//
//                    add(new BinlogTest(WRITE, "singleSet", "SET('a', 'b')", "'a'", Optional.empty(), Optional.empty()));
//                    add(new BinlogTest(WRITE, "multiSet", "SET('foobar', 'bazqux')", "'foobar'", Optional.empty(), Optional.empty()));
//                    add(new BinlogTest(WRITE, "emptySet", "SET('', 'bazqux')", "''", Optional.empty(), Optional.empty()));
//                    add(new BinlogTest(WRITE, "nullSet", "SET('foobar', 'bazqux')", "null", Optional.empty(), Optional.empty()));
//
//                    add(new BinlogTest(WRITE, "minTinyint", "TINYINT", "-128", Optional.empty(), Optional.empty()));
//                    add(new BinlogTest(WRITE, "maxTinyint", "TINYINT", "127", Optional.empty(), Optional.empty()));
//                    add(new BinlogTest(WRITE, "nullTinyint", "TINYINT", "null", Optional.empty(), Optional.empty()));
//
//                    add(new BinlogTest(WRITE, "minSmallint", "SMALLINT", "32767", Optional.empty(), Optional.empty()));
//                    add(new BinlogTest(WRITE, "maxSmallint", "SMALLINT", "-32768", Optional.empty(), Optional.empty()));
//                    add(new BinlogTest(WRITE, "nullSmallint", "SMALLINT", "null", Optional.empty(), Optional.empty()));
//
//                    add(new BinlogTest(WRITE, "minMediumint", "MEDIUMINT", "-8388608", Optional.empty(), Optional.empty()));
//                    add(new BinlogTest(WRITE, "maxMediumint", "MEDIUMINT", "8388607", Optional.empty(), Optional.empty()));
//                    add(new BinlogTest(WRITE, "nullMediumint", "MEDIUMINT", "null", Optional.empty(), Optional.empty()));
//
//                    add(new BinlogTest(WRITE, "minInt", "INT", "-2147483648", Optional.empty(), Optional.empty()));
                    add(new BinlogTest(WRITE, "maxInt", "INT", "2147483647", Optional.empty(), Optional.empty()));
//                    add(new BinlogTest(WRITE, "nullInt", "INT", "null", Optional.empty(), Optional.empty()));
//
//                    add(new BinlogTest(WRITE, "minBigint", "BIGINT", "-9223372036854775808", Optional.empty(), Optional.empty()));
//                    add(new BinlogTest(WRITE, "maxBigint", "BIGINT", "9223372036854775807", Optional.empty(), Optional.empty()));
//                    add(new BinlogTest(WRITE, "nullBigint", "BIGINT", "null", Optional.empty(), Optional.empty()));
//
//                    add(new BinlogTest(WRITE, "minDecimal", "DECIMAL(65,30)", "-99999999999999999999999999999999999.999999999999999999999999999999", Optional.empty(), Optional.empty()));
//                    add(new BinlogTest(WRITE, "maxDecimal", "DECIMAL(65,30)", "99999999999999999999999999999999999.999999999999999999999999999999", Optional.empty(), Optional.empty()));
//                    add(new BinlogTest(WRITE, "nullDecimal", "DECIMAL", "null", Optional.empty(), Optional.empty()));
//
//                    add(new BinlogTest(WRITE, "minFloat", "FLOAT", "-99999999999999999999999999999999999999", Optional.empty(), Optional.empty()));
//                    add(new BinlogTest(WRITE, "maxFloat", "FLOAT", "99999999999999999999999999999999999999", Optional.empty(), Optional.empty()));
//                    add(new BinlogTest(WRITE, "nullFloat", "FLOAT", "null", Optional.empty(), Optional.empty()));
//
//                    add(new BinlogTest(WRITE, "minDouble", "DOUBLE", "-1.7976931348623157E+308", Optional.empty(), Optional.empty()));
//                    add(new BinlogTest(WRITE, "maxDouble", "DOUBLE", "1.7976931348623157E+308", Optional.empty(), Optional.empty()));
//                    add(new BinlogTest(WRITE, "nullDouble", "DOUBLE", "null", Optional.empty(), Optional.empty()));
//
//                    add(new BinlogTest(WRITE, "trueBit", "BIT", "1", Optional.empty(), Optional.empty()));
//                    add(new BinlogTest(WRITE, "falseBit", "BIT", "0", Optional.empty(), Optional.empty()));
//                    add(new BinlogTest(WRITE, "nullBit", "BIT", "null", Optional.empty(), Optional.empty()));
//
//                    add(new BinlogTest(WRITE, "tinyintBool", "TINYINT(1)", "1", Optional.empty(), Optional.empty()));
//                    add(new BinlogTest(WRITE, "bitBool", "BIT", "1", Optional.empty(), Optional.empty()));
//
//                    add(new BinlogTest(WRITE, "minDate", "DATE", "'1000-01-01'", Optional.empty(), Optional.empty()));
//                     add(new BinlogTest(WRITE, "regDate", "DATE", "'1993-04-15'", Optional.empty(), Optional.empty()));
//                    add(new BinlogTest(WRITE, "maxDate", "DATE", "'9999-12-31'", Optional.empty(), Optional.empty()));
//                    add(new BinlogTest(WRITE, "nullDate", "DATE", "null", Optional.empty(), Optional.empty()));
////
//                    add(new BinlogTest(WRITE, "minTime", "TIME(6)", "'-838:59:59.000000'", Optional.empty(), Optional.empty()));
//                     add(new BinlogTest(WRITE, "regTime", "TIME(6)", "'12:59:59.000070'", Optional.empty(), Optional.empty()));
//                    add(new BinlogTest(WRITE, "maxTime", "TIME(6)", "'838:59:59.000000'", Optional.empty(), Optional.empty()));
//                    add(new BinlogTest(WRITE, "nullTime", "TIME(6)", "null", Optional.empty(), Optional.empty()));
//
//                    add(new BinlogTest(WRITE, "minDatetime", "DATETIME(6)", "'1000-01-01 00:00:00.0000007'", Optional.empty(), Optional.empty()));
//                     add(new BinlogTest(WRITE, "regDatetime", "DATETIME(6)", "'1993-04-15 12:59:59.000007'", Optional.empty(), Optional.empty()));
//                    add(new BinlogTest(WRITE, "maxDatetime", "DATETIME(6)", "'9999-12-31 23:59:59'", Optional.empty(), Optional.empty()));
//                    add(new BinlogTest(WRITE, "nullDatetime", "DATETIME(6)", "null", Optional.empty(), Optional.empty()));
//
//                    add(new BinlogTest(WRITE, "minTimestamp", "TIMESTAMP(6)", "'1970-01-01 00:00:01.000000'", Optional.empty(), Optional.empty()));
//                    add(new BinlogTest(WRITE, "maxTimestamp", "TIMESTAMP(6)", "'2038-01-18 03:14:07.999999'", Optional.empty(), Optional.empty()));
//                    add(new BinlogTest(WRITE, "nullTimestamp", "TIMESTAMP(6)", "null", Optional.empty(), Optional.empty()));
//
//                    add(new BinlogTest(WRITE, "nullJson", "JSON", "null", Optional.empty(), Optional.empty()));
//                    add(new BinlogTest(WRITE, "intJson", "JSON", "'1'", Optional.empty(), Optional.empty()));
//                    add(new BinlogTest(WRITE, "trueJson", "JSON", "'true'", Optional.empty(), Optional.empty()));
//                    add(new BinlogTest(WRITE, "falseJson", "JSON", "'false'", Optional.empty(), Optional.empty()));
//                    add(new BinlogTest(WRITE, "stringJson", "JSON", "'\"a\"'", Optional.empty(), Optional.empty()));
//                    add(new BinlogTest(WRITE, "emptyObjectJson", "JSON", "'{}'", Optional.empty(), Optional.empty()));
//                    add(new BinlogTest(WRITE, "emptyArrayJson", "JSON", "'[]'", Optional.empty(), Optional.empty()));
//                    add(new BinlogTest(WRITE, "basicArrayJson", "JSON", "'[1]'", Optional.empty(), Optional.empty()));
//                    add(new BinlogTest(WRITE, "complexArrayJson", "JSON", "'[1, \"a\"]'", Optional.empty(), Optional.empty()));
//                    add(new BinlogTest(WRITE, "basicObjectJson", "JSON", "'{\"a\":false}'", Optional.empty(), Optional.empty()));
//                    add(new BinlogTest(WRITE, "nestedArray1Json", "JSON", "'[[1]]'", Optional.empty(), Optional.empty()));
//                    add(new BinlogTest(WRITE, "nestedArray2Json", "JSON", "'[{\"a\":1}]'", Optional.empty(), Optional.empty()));
//                    add(new BinlogTest(WRITE, "nestedObject1Json", "JSON", "'{\"a\": {\"b\":1}}'", Optional.empty(), Optional.empty()));
//                    add(new BinlogTest(WRITE, "nestedObject2Json", "JSON", "'{\"a\":[0]}'", Optional.empty(), Optional.empty()));
                }
            };
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public BinlogTestTypes getEventType() {
        return eventType;
    }

    public String getColumn() {
        return column;
    }

    public String getType() {
        return type;
    }

    public String getValue() {
        return value;
    }

    public Optional<String> getUpdateValue() {
        return updateValue;
    }

    public Optional<String> getCharSet() {
        return charSet;
    }
}
