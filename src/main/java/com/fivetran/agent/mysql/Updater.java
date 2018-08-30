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
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import java.util.*;
import java.util.stream.Collectors;

public class Updater {

    public static final ObjectMapper JSON = new ObjectMapper();

    private static final Long PAGE_SIZE = 10_000_000L;

    private final Config config;
    protected final MysqlApi mysql;
    final Output out;
    private final Log log;
    protected final AgentState state;

    public Updater(Config config, MysqlApi mysql, Output out, Log log, AgentState state) {
        this.config = config;
        this.mysql = mysql;
        this.out = out;
        this.log = log;
        this.state = state;
    }

    public void update() throws Exception {
        updateState();

        while (true) {
            sync();
        }
    }

    void sync() throws Exception {
        TableRef tableToImport = findTableToImport();

        if (tableToImport != null)
            syncPageFromTable(tableToImport);

        syncFromBinlog(null);
    }

    TableRef findTableToImport() {
        for (TableRef tableRef : state.tableStates.keySet()) {
            if (!state.tableStates.get(tableRef).finishedImport)
                return tableRef;
        }
        return null;
    }

    void updateState() {
        updateStateTableInfo();

        if (state.binlogPosition == null)
            state.binlogPosition = mysql.readSourceLog.currentPosition();
    }

    private void updateStateTableInfo() {
        Map<TableRef, TableDefinition> allSourceTables = mysql.tableDefinitions.get();
        Map<TableRef, TableDefinition> tablesToSync = config.getTablesToSync(allSourceTables);

        Set<TableRef> newTables = Sets.difference(tablesToSync.keySet(), state.tableStates.keySet());
        newTables.forEach(t -> {
            state.tableStates.put(t, new TableState());
            state.tableDefinitions.put(t, tablesToSync.get(t));
            out.emitEvent(Event.createBeginTable(t), state);
        });

        Set<TableRef> lostTables = Sets.difference(ImmutableSet.copyOf(state.tableStates.keySet()), tablesToSync.keySet());
        lostTables.forEach(t -> {
            state.tableStates.remove(t);
            state.tableDefinitions.remove(t);
        });

        tablesToSync.forEach((tableRef, sourceTableDef) -> {
            if (state.tableDefinitions.containsKey(tableRef))
                state.tableDefinitions.put(tableRef, sourceTableDef);
        });
    }

    void syncPageFromTable(TableRef tableRef) {
        TableState tableState = state.tableStates.get(tableRef);
        TableDefinition tableDefinition = state.tableDefinitions.get(tableRef);

        List<String> hashedColumns = hashedColumns(tableRef);
        List<String> cursors = getCursors(tableRef);

        log.log(new BeginSelectPage(tableRef));
        List<ColumnDefinition> columnsToSync = config.getColumnsToSync(tableDefinition);

        List<String> columnNames = columnsToSync.stream().map(c -> c.name).collect(Collectors.toList());
        List<String> orderByColumns = columnsToSync.stream().filter(c -> c.key).map(c -> c.name).sorted().collect(Collectors.toList());

        ImportTable.PagingParams pagingParams = new ImportTable.PagingParams(orderByColumns, cursors, PAGE_SIZE);
        int rowsWritten = 0;
        try (Rows rows = mysql.importTable.rows(tableRef, columnsToSync, Optional.of(pagingParams))) {

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
            state.tableStates.get(tableRef).finishedImport = true;
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
        Optional<Map<String, String>> maybeLastSyncedPrimaryKey = state.tableStates.get(tableRef).lastSyncedPrimaryKey;
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

    void syncFromBinlog(BinlogPosition target) throws Exception {
        BinlogPosition startingPosition = state.binlogPosition;

        try (EventReader eventReader = mysql.readSourceLog.events(startingPosition)) {
            while (true) {
                SourceEvent sourceEvent = eventReader.readEvent();
                state.binlogPosition = sourceEvent.binlogPosition;

                if (desirable(sourceEvent)) {
                    if (!state.tableStates.containsKey(sourceEvent.tableRef)) {
                        updateStateTableInfo();
                        return;
                    }
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
                            throw new RuntimeException("Unexpected case: " + sourceEvent.event);
                    }
                } else {
                    out.emitEvent(Event.createNop(), state);

                    if (sourceEvent.event == SourceEventType.TIMEOUT && state.tableStates.values().stream().anyMatch(tableState -> !tableState.finishedImport))
                        return;
                }
                if (target != null && target.equals(sourceEvent.binlogPosition))
                    return;
            }
        }
    }

    // TODO review this carefully.
    private boolean desirable(SourceEvent sourceEvent) {
        return sourceEvent.event != SourceEventType.TIMEOUT
                && sourceEvent.event != SourceEventType.OTHER
                && config.selectable(sourceEvent.tableRef);
    }

    private void emitFromInsert(SourceEvent event) {
        for (Row row : event.newRows) {
            if (state.tableDefinitions.get(event.tableRef).columns.size() != row.getColumnCount()) {
                updateStateTableInfo();
                state.tableStates.put(event.tableRef, new TableState());
                out.emitEvent(Event.createBeginTable(event.tableRef), state);
            }
            out.emitEvent(Event.createUpsert(event.tableRef, row), state);
        }
    }

    private void emitFromUpdate(SourceEvent event) {
        for (int i = 0; i < event.oldRows.size(); ++i) {
            if (state.tableDefinitions.get(event.tableRef).columns.size() != event.oldRows.get(i).getColumnCount()) {
                updateStateTableInfo();
                state.tableStates.put(event.tableRef, new TableState());
                out.emitEvent(Event.createBeginTable(event.tableRef), state);
            }
            out.emitEvent(Event.createDelete(event.tableRef, event.oldRows.get(i)), state);
            out.emitEvent(Event.createUpsert(event.tableRef, event.newRows.get(i)), state);
        }
    }

    private void emitFromDelete(SourceEvent event) {
        for (Row row : event.newRows) {
            if (state.tableDefinitions.get(event.tableRef).columns.size() != row.getColumnCount())
                updateStateTableInfo();
            out.emitEvent(Event.createDelete(event.tableRef, row), state);
        }
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