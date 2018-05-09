package com.fivetran.agent.mysql;

import com.fivetran.agent.mysql.config.Config;
import com.fivetran.agent.mysql.config.SchemaConfig;
import com.fivetran.agent.mysql.config.TableConfig;
import com.fivetran.agent.mysql.log.LogMessage;
import com.fivetran.agent.mysql.output.ColumnDefinition;
import com.fivetran.agent.mysql.output.Event;
import com.fivetran.agent.mysql.output.TableDefinition;
import com.fivetran.agent.mysql.source.BinlogPosition;
import com.fivetran.agent.mysql.source.Row;
import com.fivetran.agent.mysql.source.SourceEvent;
import com.fivetran.agent.mysql.source.TableRef;
import com.fivetran.agent.mysql.source.binlog.client.EventReader;
import com.fivetran.agent.mysql.state.AgentState;
import com.fivetran.agent.mysql.state.TableState;
import com.google.common.collect.ImmutableList;
import org.junit.Ignore;
import org.junit.Test;

import java.util.*;
import java.util.function.Supplier;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.*;

public class BatchUpdaterSpec {
    private Config config = new Config();
    private List<Row> readRows = new ArrayList<>();
    private List<SourceEvent> sourceEvents = new ArrayList<>();
    private Rows rows = new Rows() {
        @Override
        public List<String> columnNames() {
            return null;
        }

        @Override
        public void close() {

        }

        @Override
        public Iterator<Row> iterator() {
            return readRows.iterator();
        }
    };
    private BinlogPosition binlogPosition = new BinlogPosition("mysql-bin-changelog.000001", 1L);
    private Map<TableRef, TableDefinition> tableDefinitions = new HashMap<>();
    private AgentState state = new AgentState();
    private ReadSourceLog read = new ReadSourceLog() {
        @Override
        public BinlogPosition currentPosition() {
            return binlogPosition;
        }

        @Override
        public EventReader events(BinlogPosition startPosition) {
            Iterator<SourceEvent> sourceEventIterable = sourceEvents.iterator();
            return new EventReader() {
                @Override
                public SourceEvent readEvent() {
                    if (sourceEventIterable.hasNext())
                        return sourceEventIterable.next();
                    else
                        return SourceEvent.createTimeout(binlogPosition);
                }

                @Override
                public void close() {

                }
            };
        }
    };
    private ImportTable importTable = (table, selectColumns, pagingParams) -> rows;
    private MysqlApi api = new MysqlApi(importTable, null, null, () -> tableDefinitions, read);
    private final List<Event> outEvents = new ArrayList<>();
    private final Output out = new Output() {
        @Override
        public void emitEvent(Event event, AgentState state) {
            outEvents.add(event);
        }

        @Override
        public void close() {

        }
    };
    private final List<LogMessage> logMessages = new ArrayList<>();

    @Test
    public void update_onlySyncSelectedTablesWithDefaultConfigValues() throws Exception {
        TableRef selectedTable = new TableRef("test_schema", "selected_table");
        TableRef ignoredTable = new TableRef("test_schema", "ignored_table");

        Config config = new Config();
        config.schemas.put("test_schema", new SchemaConfig());
        config.schemas.get("test_schema").tables.put("selected_table", new TableConfig());
        config.schemas.get("test_schema").tables.put("ignored_table", new TableConfig());
        config.schemas.get("test_schema").tables.get("ignored_table").selected = false;

        Row row1 = row("1", "foo-1"), row2 = row("2", "foo-2");
        readRows = ImmutableList.of(row1, row2);

        tableDefinitions.clear();
        TableDefinition selectedTableDef = new TableDefinition(selectedTable, Arrays.asList(new ColumnDefinition("id", "text", true), new ColumnDefinition("data", "text", false)));
        tableDefinitions.put(selectedTable, selectedTableDef);
        TableDefinition ignoredTableDef = new TableDefinition(ignoredTable, Arrays.asList(new ColumnDefinition("id", "text", true), new ColumnDefinition("data", "text", false)));
        tableDefinitions.put(ignoredTable, ignoredTableDef);

        new BatchUpdater(config, api, out, logMessages::add, state).update();

        assertEquals(outEvents.size(), 4);
        assertTrue(outEvents.contains(Event.createUpsert(selectedTable, row1)));
        assertTrue(outEvents.contains(Event.createUpsert(selectedTable, row2)));
        assertTrue(outEvents.contains(Event.createNop()));
        assertTrue(outEvents.contains(Event.createTableDefinition(selectedTableDef)));
    }

    @Test
    public void update_fullSelectSyncThenBinlog() throws Exception {
        TableRef tableRef = new TableRef("test_schema", "test_table");
        tableDefinitions.clear();
        TableDefinition selectedTableDef = new TableDefinition(tableRef, Arrays.asList(new ColumnDefinition("id", "text", true), new ColumnDefinition("data", "text", false)));
        tableDefinitions.put(tableRef, selectedTableDef);

        Row row1 = row("1", "foo-1"), row2 = row("2", "foo-2"), row3 = row("3", "foo-3"), row4 = row("4", "foo-4");
        readRows = ImmutableList.of(row1, row2);

        BatchUpdater initialImport = new BatchUpdater(config, api, out, logMessages::add, state);
        initialImport.update();

        assertThat(outEvents.size(), equalTo(4));
        assertTrue(outEvents.contains(Event.createUpsert(tableRef, row1)));
        assertTrue(outEvents.contains(Event.createUpsert(tableRef, row2)));
        assertTrue(outEvents.contains(Event.createNop()));
        assertTrue(outEvents.contains(Event.createTableDefinition(selectedTableDef)));

        outEvents.clear();

        SourceEvent event1 = SourceEvent.createInsert(tableRef, new BinlogPosition("mysql-bin-changelog.000001", 2L), ImmutableList.of(row3));
        SourceEvent event2 = SourceEvent.createInsert(tableRef, new BinlogPosition("mysql-bin-changelog.000001", 3L), ImmutableList.of(row4));
        binlogPosition = new BinlogPosition("mysql-bin-changelog.000001", 4L);
        sourceEvents = ImmutableList.of(event1, event2);

        BatchUpdater incrementalUpdate = new BatchUpdater(config, api, out, logMessages::add, state);
        incrementalUpdate.update();

        assertThat(outEvents.size(), equalTo(3));

        assertTrue(outEvents.contains(Event.createUpsert(tableRef, row3)));
        assertTrue(outEvents.contains(Event.createUpsert(tableRef, row4)));
        assertTrue(outEvents.contains(Event.createNop()));
    }

    @Test
    public void update_syncHashedColumns() throws Exception {
        TableRef tableRef = new TableRef("test_schema", "test_table");
        Config config = new Config();
        config.cryptoSalt = "sodium chloride";
        config.putColumn(config, tableRef, "id", true, true);

        Row row1 = row("1", "foo-1"), row2 = row("2", "foo-2");
        readRows = ImmutableList.of(row1, row2);

        tableDefinitions.clear();
        tableDefinitions.put(tableRef, new TableDefinition(tableRef, Arrays.asList(new ColumnDefinition("id", "text", true), new ColumnDefinition("data", "text", false))));

        BatchUpdater updater = new BatchUpdater(config, api, out, logMessages::add, state);
        updater.update();

        assertFalse(outEvents.contains(Event.createUpsert(tableRef, Arrays.asList("1", "foo-1"))));
        assertTrue(outEvents.contains(Event.createUpsert(tableRef, Arrays.asList("AnZXUjEr5i2a57kXUtI6dXftv+E=", "foo-1"))));
        assertTrue(outEvents.contains(Event.createUpsert(tableRef, Arrays.asList("jUdn9gOx372QFiYR9zRyZp0VQEw=", "foo-2"))));
    }

    @Test
    public void binlogSync_updateTableDefinition() throws Exception {
        TableRef tableRef = new TableRef("test_schema", "test_table");
        AgentState state = new AgentState();
        TableState tableState = new TableState();

        tableState.finishedImport = true;
        state.tables.put(tableRef, tableState);
        state.binlogPosition = new BinlogPosition("mysql-bin-changelog.000001", 1L);

        TableDefinition initialTableDef = new TableDefinition(tableRef, Arrays.asList(new ColumnDefinition("id", "text", true), new ColumnDefinition("data", "text", false)));
        TableDefinition updatedTableDef = new TableDefinition(tableRef, Arrays.asList(new ColumnDefinition("id", "text", true), new ColumnDefinition("data", "text", false), new ColumnDefinition("update", "text", false)));

        Supplier<Map<TableRef, TableDefinition>> updatableTableDefinitions = () -> {
            if (outEvents.size() == 0)
                return Collections.singletonMap(tableRef, initialTableDef);
            else
                return Collections.singletonMap(tableRef, updatedTableDef);
        };
        MysqlApi customApi = new MysqlApi(importTable, null, null, updatableTableDefinitions, read);

        Row standardRow = new Row("1", "foo-1"), modifiedRow = new Row("2", "foo-2", "bar-3");

        SourceEvent standardEvent = SourceEvent.createInsert(tableRef, new BinlogPosition("mysql-bin-changelog.000001", 2L), ImmutableList.of(standardRow));
        SourceEvent modifiedEvent = SourceEvent.createInsert(tableRef, new BinlogPosition("mysql-bin-changelog.000001", 3L), ImmutableList.of(modifiedRow));
        sourceEvents = ImmutableList.of(standardEvent, modifiedEvent);
        binlogPosition = new BinlogPosition("mysql-bin-changelog.000001", 4L);

        new BatchUpdater(config, customApi, out, logMessages::add, state).update();

        assertEquals(outEvents.size(), 4);
        assertTrue(outEvents.contains(Event.createUpsert(tableRef, standardRow)));
        assertTrue(outEvents.contains(Event.createUpsert(tableRef, modifiedRow)));
        assertTrue(outEvents.contains(Event.createNop()));
        assertTrue(outEvents.contains(Event.createTableDefinition(updatedTableDef)));
    }

    @Test
    public void binlogSync_allBinlogEvents() throws Exception {
        TableRef tableRef = new TableRef("test_schema", "test_table");
        AgentState state = new AgentState();
        TableState tableState = new TableState();

        tableState.finishedImport = true;
        state.tables.put(tableRef, tableState);
        state.binlogPosition = new BinlogPosition("mysql-bin-changelog.000001", 1L);

        tableDefinitions.clear();
        TableDefinition tableDef = new TableDefinition(tableRef, Arrays.asList(new ColumnDefinition("id", "text", true), new ColumnDefinition("data", "text", false)));
        tableDefinitions.put(tableRef, tableDef);

        Row insertedRow = new Row("1", "foo-1"), updatedRow = new Row("2", "foo-2");

        SourceEvent insert = SourceEvent.createInsert(tableRef, new BinlogPosition("mysql-bin-changelog.000001", 2L), ImmutableList.of(insertedRow));
        SourceEvent other1 = SourceEvent.createOther(new BinlogPosition("mysql-bin-changelog.000001", 3L));
        SourceEvent update = SourceEvent.createUpdate(tableRef, new BinlogPosition("mysql-bin-changelog.000001", 4L), ImmutableList.of(insertedRow), ImmutableList.of(updatedRow));
        SourceEvent other2 = SourceEvent.createOther(new BinlogPosition("mysql-bin-changelog.000001", 5L));
        SourceEvent delete = SourceEvent.createDelete(tableRef, new BinlogPosition("mysql-bin-changelog.000001", 6L), ImmutableList.of(updatedRow));
        sourceEvents = ImmutableList.of(insert, other1, update, other2, delete);
        binlogPosition = new BinlogPosition("mysql-bin-changelog.000001", 7L);

        new BatchUpdater(config, api, out, logMessages::add, state).update();

        assertEquals(outEvents.size(), 7);
        assertTrue(outEvents.contains(Event.createUpsert(tableRef, insertedRow)));
        assertTrue(outEvents.contains(Event.createNop()));
        assertTrue(outEvents.contains(Event.createDelete(tableRef, insertedRow)));
        assertTrue(outEvents.contains(Event.createUpsert(tableRef, updatedRow)));
        assertTrue(outEvents.contains(Event.createNop()));
        assertTrue(outEvents.contains(Event.createDelete(tableRef, updatedRow)));
        assertTrue(outEvents.contains(Event.createNop()));
    }

    // TODO we would have to mimick the paging param code to make this work. Is it worth it?
    // TODO how do we incrementally update hashed keys?
    @Ignore
    @Test
    public void importSync_withExistingSelectState() throws Exception {
        TableRef tableRef = new TableRef("test_schema", "test_table");
        AgentState state = new AgentState();
        TableState tableState = new TableState();

        tableState.lastSyncedPrimaryKey = Optional.of(Collections.singletonMap("id", "3"));
        state.tables.put(tableRef, tableState);

        tableDefinitions.clear();
        TableDefinition selectedTableDef = new TableDefinition(tableRef, Arrays.asList(new ColumnDefinition("id", "text", true), new ColumnDefinition("data", "text", false)));
        tableDefinitions.put(tableRef, selectedTableDef);

        Row row1 = row("1", "foo-1"), row2 = row("2", "foo-2"), row3 = row("3", "foo-3"), row4 = row("4", "I pity the foo");
        readRows = ImmutableList.of(row1, row2, row3, row4);

        BatchUpdater updater = new BatchUpdater(config, api, out, logMessages::add, state);

        updater.update();

        System.out.println();
    }

    private Row row(String... values) {
        Row row = new Row(values.length);
        row.addAll(Arrays.asList(values));
        return row;
    }
}
