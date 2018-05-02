package com.fivetran.agent.mysql.source.binlog.parser;

import com.fivetran.agent.mysql.source.Row;
import com.fivetran.agent.mysql.source.TableRef;
import com.fivetran.agent.mysql.source.binlog.BinlogInputStream;
import com.fivetran.agent.mysql.source.binlog.parser.shyiko.ColumnType;
import com.fivetran.agent.mysql.source.binlog.parser.shyiko.RowParser;

import java.io.IOException;
import java.util.*;

import static com.fivetran.agent.mysql.source.binlog.client.BinlogClient.*;

public class EventBodyParser {

    public EventBody parse(byte[] input, EventType type, Map<Long, TableMapEventBody> tableMaps) throws IOException {
        BinlogInputStream in = new BinlogInputStream(input);

        switch (type) {
            case EXT_WRITE_ROWS:
            case EXT_DELETE_ROWS:
                return parseWriteDeleteEvent(in, tableMaps);
            case EXT_UPDATE_ROWS:
                return parseUpdateEvent(in, tableMaps);
            case ROTATE:
                return parseRotateEvent(in);
            case TABLE_MAP:
                TableMapEventBody tableMapEventBody = parseTableMapEvent(in);
                tableMaps.put(tableMapEventBody.getTableId(), tableMapEventBody);
                return null;
            case XID:  // MySQL does not allow nested transactions so we can clear tableMaps at the end of each transaction
                tableMaps.clear();
                return null;
            default:
                return null;
        }
    }

    private EventBody parseWriteDeleteEvent(BinlogInputStream in,
                                            Map<Long, TableMapEventBody> tableMaps) throws IOException {
        ModifyingEventBody event = new ModifyingEventBody();

        parseSimpleData(in, event);
        setTableRef(event, tableMaps);

        event.setIncludedColumns(in.readBitSet(event.getColumnCount(), true));
        findNewRows(in, tableMaps, event);

        return event;
    }

    private EventBody parseUpdateEvent(BinlogInputStream in,
                                       Map<Long, TableMapEventBody> tableMaps) throws IOException {
        ModifyingEventBody event = new ModifyingEventBody();

        parseSimpleData(in, event);
        setTableRef(event, tableMaps);

        // Skip unnecessary old values
        in.readBitSet(event.getColumnCount(), true);

        event.setIncludedColumns(in.readBitSet(event.getColumnCount(), true));

        findOldNewRows(in, tableMaps, event);

        return event;
    }

    private void findOldNewRows(BinlogInputStream in,
                                Map<Long, TableMapEventBody> tableMaps,
                                ModifyingEventBody event) throws IOException {
        RowParser rowParser = new RowParser();
        List<Row> oldRows = new ArrayList<>();
        List<Row> newRows = new ArrayList<>();

        while (in.available() > 0) {
            Row oldRow = new Row();
            Row newRow = new Row();

            TableMapEventBody tableMapEventBody = tableMaps.get(event.getTableId());

            Collections.addAll(oldRow,
                    rowParser.parseRow(in, tableMapEventBody, event.getIncludedColumns()));

            Collections.addAll(newRow,
                    rowParser.parseRow(in, tableMapEventBody, event.getIncludedColumns()));

            oldRows.add(oldRow);
            newRows.add(newRow);
        }
        if (oldRows.size() != newRows.size()) {
            throw new RuntimeException("Error in parsing updated rows. " +
                    "Number of old rows does not match number of new rows");
        }
        event.setOldRows(oldRows);
        event.setNewRows(newRows);
    }

    private void findNewRows(
            BinlogInputStream in,
            Map<Long, TableMapEventBody> tableMaps,
            ModifyingEventBody event) throws IOException {
        RowParser rowParser = new RowParser();
        List<Row> rows = new ArrayList<>();

        while (in.available() > 0) {
            Row row = new Row();
            TableMapEventBody tableMapEventBody = tableMaps.get(event.getTableId());

            Collections.addAll(row,
                    rowParser.parseRow(in, tableMapEventBody, event.getIncludedColumns()));
            rows.add(row);
        }
        event.setNewRows(rows);
    }

    private void parseSimpleData(BinlogInputStream in,
                                 ModifyingEventBody event) {
        event.setTableId(findTableId(in));

        // Skip unused extra bytes if present. Only for EventTypes prepended by 'EXT_'
        int extraInfoLength = in.readInteger(TWO_BYTES);
        in.skip(extraInfoLength - TWO_BYTES);

        event.setColumnCount(in.readPackedInteger());
    }

    private void setTableRef(ModifyingEventBody event, Map<Long, TableMapEventBody> tableMaps) {
        TableMapEventBody tableMapEventBody = tableMaps.get(event.getTableId());
        event.setTableRef(tableMapEventBody.getTableRef());
    }

    private TableMapEventBody parseTableMapEvent(BinlogInputStream in) {
        TableMapEventBody tableMapEvent = new TableMapEventBody();
        String schemaName;
        String tableName;
        int columnCount;

        tableMapEvent.setTableId(findTableId(in));
        // Skip unused schema name length
        in.skip(ONE_BYTE);

        schemaName = in.readZeroTerminatedString();
        // Skip unused table name length
        in.skip(ONE_BYTE);

        tableName = in.readZeroTerminatedString();
        columnCount = in.readPackedInteger();

        // To find more on MySQL column types look at enum_field_types in mysql_com.h on the MySQL repo
        tableMapEvent.setColumnTypes(in.read(columnCount));
        // Skip unused metadata
        in.skipPackedInteger();

        // See log_event.h on the MySQL repo for contents and format
        tableMapEvent.setColumnMetadata(readColumnMetadata(in, tableMapEvent.getColumnTypes()));

        // Skip unused column nullability
        in.readBitSet(columnCount, true);

        tableMapEvent.setTableRef(new TableRef(schemaName, tableName));

        return tableMapEvent;
    }

    // Column metadata required to parse row values in the RowParser
    private int[] readColumnMetadata(BinlogInputStream inputStream, byte[] columnTypes) {
        int[] metadata = new int[columnTypes.length];
        for (int i = 0; i < columnTypes.length; i++) {
            switch (ColumnType.byCode(columnTypes[i] & 0xFF)) {
                case FLOAT:
                case DOUBLE:
                case JSON:
                case BLOB:
                    metadata[i] = inputStream.readInteger(ONE_BYTE);
                    break;
                case BIT:
                case VARCHAR:
                case NEWDECIMAL:
                    metadata[i] = inputStream.readInteger(TWO_BYTES);
                    break;
                case SET:
                case ENUM:
                case STRING:
                    metadata[i] = RowParser.bigEndianInteger(inputStream.read(TWO_BYTES), 0, TWO_BYTES);
                    break;
                case TIME_V2:
                case DATETIME_V2:
                case TIMESTAMP_V2:
                    metadata[i] = inputStream.readInteger(ONE_BYTE);
                    break;
                default:
                    metadata[i] = 0;
            }
        }
        return metadata;
    }

    private long findTableId(BinlogInputStream in) {
        long tableId = in.readLong(SIX_BYTES);

        // Skip unused reserved bytes
        in.skip(TWO_BYTES);

        return tableId;
    }

    private EventBody parseRotateEvent(BinlogInputStream in) {
        RotateEventBody rotateEventBody = new RotateEventBody();
        rotateEventBody.setBinlogPosition(in.readLong(EIGHT_BYTES));
        rotateEventBody.setBinlogFilename(in.readString(in.available()));
        return rotateEventBody;
    }
}
