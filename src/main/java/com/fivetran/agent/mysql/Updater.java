/**
* Copyright (c) Fivetran 2018
**/
package com.fivetran.agent.mysql;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fivetran.agent.mysql.config.Config;
import com.fivetran.agent.mysql.config.TableConfig;
import com.fivetran.agent.mysql.hash.Hash;
import com.fivetran.agent.mysql.log.BeginSelectPage;
import com.fivetran.agent.mysql.output.ColumnDefinition;
import com.fivetran.agent.mysql.output.Event;
import com.fivetran.agent.mysql.output.TableDefinition;
import com.fivetran.agent.mysql.source.*;
import com.fivetran.agent.mysql.source.binlog.client.EventReader;
import com.fivetran.agent.mysql.state.AgentState;
import com.fivetran.agent.mysql.state.TableState;
import com.fivetran.agent.mysql.utils.AgentUtils;
import com.google.common.collect.Sets;

import java.util.*;
import java.util.stream.Collectors;

public class Updater {

    public static final ObjectMapper JSON = new ObjectMapper();

    private static final Long PAGE_SIZE = 10_000_000L;

    private final Config config;
    private final MysqlApi mysql;
    final Output out;
    private final Log log;
    protected final AgentState state;
    Map<TableRef, TableDefinition> tablesToSync;

    public Updater(Config config, MysqlApi mysql, Output out, Log log, AgentState state) {
        this.config = config;
        this.mysql = mysql;
        this.out = out;
        this.log = log;
        this.state = state;
    }

    /**
     * Perform one cycle of update
     */
    public void update() throws Exception {
        updateTableDefinitions();

        while (true) {
            sync();
        }
    }

    void sync() throws Exception {
        TableRef tableToImport = findTableToImport();
        syncPageFromTable(tablesToSync.get(tableToImport));
        syncFromBinlog();
    }

    TableRef findTableToImport() {
        for (TableRef tableRef : state.tables.keySet()) {
            if (!state.tables.get(tableRef).finishedImport)
                return tableRef;
        }

        throw new RuntimeException("There should have been tables to sync, but none were found");
    }

    private void updateState(Set<TableRef> tablesToSync) {
        Set<TableRef> newTables = Sets.difference(tablesToSync, state.tables.keySet());
        newTables.forEach(t -> state.tables.put(t, new TableState()));

        Set<TableRef> lostTables = Sets.difference(state.tables.keySet(), tablesToSync);
        lostTables.forEach(state.tables::remove);

        if (state.binlogPosition == null)
            state.binlogPosition = mysql.readSourceLog.currentPosition();
    }

    void syncPageFromTable(TableDefinition tableDefinition) {
        TableRef tableRef = tableDefinition.table;
        TableState tableState = state.tables.get(tableRef);
        List<String> hashedColumns = hashedColumns(tableRef);

        out.emitEvent(Event.createTableDefinition(tableDefinition), state);
        List<String> cursors = getCursors(tableRef);

        log.log(new BeginSelectPage(tableRef));
        List<ColumnDefinition> columnsToSync = config.getColumnsToSync(tableDefinition);

        List<String> columnNames = columnsToSync.stream().map(c -> c.name).collect(Collectors.toList());
        List<String> orderByColumns = columnsToSync.stream().filter(c -> c.key).map(c -> c.name).sorted().collect(Collectors.toList());

        ImportTable.PagingParams pagingParams = new ImportTable.PagingParams(orderByColumns, cursors, PAGE_SIZE);
        int rowsWritten = 0;
        try (Rows rows = mysql.importTable.rows(tableRef, columnNames, Optional.of(pagingParams))) {

            List<Integer> keyIndices = getIndices(columnNames, orderByColumns);
            List<Integer> hashIndices = getIndices(columnNames, hashedColumns);

            for (Row row : rows) {
                cursors.clear();

                for (Integer keyIndex : keyIndices)
                    cursors.add(row.get(keyIndex));

                for (int hashIndex : hashIndices) {
                    String valueToHash = row.remove(hashIndex);
                    row.add(hashIndex, Hash.hash(config.cryptoSalt + valueToHash));
                }

                tableState.lastSyncedPrimaryKey = Optional.of(AgentUtils.map(orderByColumns, cursors));
                out.emitEvent(Event.createUpsert(tableRef, row), state);

                if (rowsWritten++ == PAGE_SIZE)
                    return;
            }
            state.tables.get(tableRef).finishedImport = true;
        }
    }

    private List<Integer> getIndices(List<String> allColumns, List<String> specialColumns) {
        List<Integer> indices = new ArrayList<>();
        for (int i = 0; i < allColumns.size(); i++) {
            if (specialColumns.contains(allColumns.get(i)))
                indices.add(i);
        }
        return indices;
    }

    private List<String> getCursors(TableRef tableRef) {
        List<String> cursors = new ArrayList<>();
        Optional<Map<String, String>> maybeLastSyncedPrimaryKey = state.tables.get(tableRef).lastSyncedPrimaryKey;
        if (!maybeLastSyncedPrimaryKey.isPresent())
            return cursors;

        Map<String, String> lastSyncedPrimaryKey = maybeLastSyncedPrimaryKey.get();
        List<String> sortedColumnNames = lastSyncedPrimaryKey.keySet().stream().sorted().collect(Collectors.toList());
        for (String columnName : sortedColumnNames) {
            String value = lastSyncedPrimaryKey.get(columnName);
            cursors.add(value);
        }
        return cursors;
    }


    private void syncFromBinlog() throws Exception {
        syncFromBinlog(null);
    }

    void syncFromBinlog(BinlogPosition target) throws Exception {
        BinlogPosition startingPosition = state.binlogPosition;

        try (EventReader eventReader = mysql.readSourceLog.events(startingPosition)) {
            while (true) {
                SourceEvent sourceEvent = eventReader.readEvent();

                if (target != null && sourceEvent.binlogPosition.equals(target))
                    return;

                if (sourceEvent.event == SourceEventType.TIMEOUT && state.tables.values().stream().anyMatch(tableState -> !tableState.finishedImport))
                    return;

                if (sourceEvent.event != SourceEventType.TIMEOUT && !state.tables.containsKey(sourceEvent.tableRef)) {
                    updateTableDefinitions();
                    return;
                }
                if (sourceEvent.event == SourceEventType.TIMEOUT)
                    out.emitEvent(Event.createNop(), state);

                if (sourceEvent.event != SourceEventType.TIMEOUT) {
                    // todo manage when we emit tableDefinition events so that there is always a relevant table definition before an event in a file

                    // todo: in cases when we cannot reconcile table definitions, error out and print message to resync table
                    switch (sourceEvent.event) {
                        case INSERT:
                            emitFromInsert(sourceEvent);
                            break;
                        case UPDATE:
                            emitFromUpdate(sourceEvent);
                            break;
                        case DELETE:
                            emitFromDelete(sourceEvent);
                            break;
                        default:
                            throw new RuntimeException("Unexpected switch case for source event type " + sourceEvent.event);
                    }
                }
                state.binlogPosition = sourceEvent.binlogPosition;
            }
        }
    }

    private void emitFromInsert(SourceEvent event) {
        for (Row row : event.newRows) {
            if (tablesToSync.get(event.tableRef).columns.size() != row.getColumnCount()) {
                updateTableDefinitions();
                out.emitEvent(Event.createTableDefinition(tablesToSync.get(event.tableRef)), state);
            }
            out.emitEvent(Event.createUpsert(event.tableRef, row), state);
        }
    }

    private void emitFromUpdate(SourceEvent event) {
        for (int i = 0; i < event.oldRows.size(); ++i) {
            if (tablesToSync.get(event.tableRef).columns.size() != event.oldRows.get(i).getColumnCount()) {
                updateTableDefinitions();
                out.emitEvent(Event.createTableDefinition(tablesToSync.get(event.tableRef)), state);
            }
            out.emitEvent(Event.createDelete(event.tableRef, event.oldRows.get(i)), state);
            out.emitEvent(Event.createUpsert(event.tableRef, event.newRows.get(i)), state);
        }
    }

    private void emitFromDelete(SourceEvent event) {
        for (Row row : event.newRows) {
            if (tablesToSync.get(event.tableRef).columns.size() != row.getColumnCount()) {
                updateTableDefinitions();
                out.emitEvent(Event.createTableDefinition(tablesToSync.get(event.tableRef)), state);
            }
            out.emitEvent(Event.createDelete(event.tableRef, row), state);
        }
    }

    void updateTableDefinitions() {
        Map<TableRef, TableDefinition> allSourceTables = mysql.tableDefinitions.get();

        tablesToSync = config.getTablesToSync(allSourceTables);

        updateState(tablesToSync.keySet());
    }

    private List<String> hashedColumns(TableRef tableRef) {
        List<String> hashedColumns = new ArrayList<>();
        Optional<TableConfig> maybeTableConfig = config.getTable(tableRef);

        if (maybeTableConfig.isPresent()) {
            TableConfig tableConfig = maybeTableConfig.get();
            for (String columnName : tableConfig.columns.keySet()) {
                if (tableConfig.columns.get(columnName).hash)
                    hashedColumns.add(columnName);
            }
        }
        return hashedColumns;
    }
}