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
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.*;
import java.util.function.Supplier;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.*;

public class BatchUpdaterTest {

    private Config config = new Config();
    private List<Row> readRows = new ArrayList<>();
    private List<SourceEvent> sourceEvents = new ArrayList<>();
    private Iterator<SourceEvent> sourceEventIterator = sourceEvents.iterator();
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
            return new EventReader() {
                @Override
                public SourceEvent readEvent() {
                    if (sourceEventIterator.hasNext())
                        return sourceEventIterator.next();
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

    @Before
    public void before() {
        tableDefinitions.clear();
    }

    @Test
    public void update_onlySyncSelectedTablesWithDefaultConfigValues() throws Exception {
        TableRef selectedTable = new TableRef("test_schema", "selected_table");
        TableRef ignoredTable = new TableRef("test_schema", "ignored_table");

        config.schemas.put("test_schema", new SchemaConfig());
        config.schemas.get("test_schema").tables.put("selected_table", new TableConfig());
        config.schemas.get("test_schema").tables.put("ignored_table", new TableConfig());
        config.schemas.get("test_schema").tables.get("ignored_table").selected = false;

        Row row1 = row("1", "foo-1"), row2 = row("2", "foo-2");
        readRows = ImmutableList.of(row1, row2);

        TableDefinition selectedTableDef = new TableDefinition(selectedTable, Arrays.asList(new ColumnDefinition("id", "text", true), new ColumnDefinition("data", "text", false)));
        TableDefinition ignoredTableDef = new TableDefinition(ignoredTable, Arrays.asList(new ColumnDefinition("id", "text", true), new ColumnDefinition("data", "text", false)));
        tableDefinitions.put(selectedTable, selectedTableDef);
        tableDefinitions.put(ignoredTable, ignoredTableDef);

        new BatchUpdater(config, api, out, logMessages::add, state).update();

        assertEquals(outEvents.size(), 3);
        assertTrue(outEvents.contains(Event.createUpsert(selectedTable, row1)));
        assertTrue(outEvents.contains(Event.createUpsert(selectedTable, row2)));
        assertTrue(outEvents.contains(Event.createNop()));
    }

    @Test
    public void update_fullSelectSyncThenBinlog() throws Exception {
        TableRef tableRef = new TableRef("test_schema", "test_table");

        config.schemas.put("test_schema", new SchemaConfig());
        config.schemas.get("test_schema").selectOtherTables = true;

        TableDefinition selectedTableDef = new TableDefinition(tableRef, Arrays.asList(new ColumnDefinition("id", "text", true), new ColumnDefinition("data", "text", false)));
        tableDefinitions.put(tableRef, selectedTableDef);

        Row row1 = row("1", "foo-1"), row2 = row("2", "foo-2"), row3 = row("3", "foo-3"), row4 = row("4", "foo-4");
        readRows = ImmutableList.of(row1, row2);

        BatchUpdater initialImport = new BatchUpdater(config, api, out, logMessages::add, state);
        initialImport.update();

        assertThat(outEvents.size(), equalTo(3));
        assertTrue(outEvents.contains(Event.createUpsert(tableRef, row1)));
        assertTrue(outEvents.contains(Event.createUpsert(tableRef, row2)));
        assertTrue(outEvents.contains(Event.createNop()));

        outEvents.clear();

        SourceEvent event1 = SourceEvent.createInsert(tableRef, new BinlogPosition("mysql-bin-changelog.000001", 2L), ImmutableList.of(row3));
        SourceEvent event2 = SourceEvent.createInsert(tableRef, new BinlogPosition("mysql-bin-changelog.000001", 3L), ImmutableList.of(row4));
        binlogPosition = new BinlogPosition("mysql-bin-changelog.000001", 4L);
        sourceEvents = ImmutableList.of(event1, event2);
        sourceEventIterator = sourceEvents.iterator();

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
        TableDefinition tableDef = new TableDefinition(tableRef, Arrays.asList(new ColumnDefinition("id", "text", true), new ColumnDefinition("data", "text", false)));

        tableDefinitions.put(tableRef, tableDef);
        config.putColumn(config, tableRef, "id", true, true);
        config.cryptoSalt = "sodium chloride";

        Row row1 = row("1", "foo-1"), row2 = row("2", "foo-2");
        readRows = ImmutableList.of(row1, row2);

        BatchUpdater updater = new BatchUpdater(config, api, out, logMessages::add, state);
        updater.update();

        assertFalse(outEvents.contains(Event.createUpsert(tableRef, Arrays.asList("1", "foo-1"))));
        assertTrue(outEvents.contains(Event.createUpsert(tableRef, Arrays.asList("AnZXUjEr5i2a57kXUtI6dXftv+E=", "foo-1"))));
        assertTrue(outEvents.contains(Event.createUpsert(tableRef, Arrays.asList("jUdn9gOx372QFiYR9zRyZp0VQEw=", "foo-2"))));
    }

    /* Note - in real operation, this test would re-import the table. But the test components we're using
     * aren't as smart as the real updater, and don't actually supply the records for the re-import.
     * For this test, it is sufficient to test that we upsert all the incremental rows and include a begin_table
     * event. Other tests will ensure the correctness of the table re-import mechanism.
     */
    @Test
    public void binlogSync_updateTableDefinition() throws Exception {
        TableRef tableRef = new TableRef("test_schema", "test_table");
        TableDefinition initialTableDef = new TableDefinition(tableRef, Arrays.asList(new ColumnDefinition("id", "text", true), new ColumnDefinition("data", "text", false)));
        TableDefinition updatedTableDef = new TableDefinition(tableRef, Arrays.asList(new ColumnDefinition("id", "text", true), new ColumnDefinition("data", "text", false), new ColumnDefinition("update", "text", false)));

        config.schemas.put("test_schema", new SchemaConfig());
        config.schemas.get("test_schema").selectOtherTables = true;

        AgentState state = new AgentState();
        TableState tableState = new TableState();

        tableState.finishedImport = true;
        state.tableStates.put(tableRef, tableState);
        state.tableDefinitions.put(tableRef, initialTableDef);
        state.binlogPosition = new BinlogPosition("mysql-bin-changelog.000001", 1L);

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
        sourceEventIterator = sourceEvents.iterator();
        new BatchUpdater(config, customApi, out, logMessages::add, state).update();

        // First upsert occurs as part of normal binlog processing
        assertTrue(outEvents.contains(Event.createUpsert(tableRef, standardRow)));
        // Create begin table occurs because table definition changed and we must resync
        assertTrue(outEvents.contains(Event.createBeginTable(tableRef)));
        // Second upsert occurs when we sync the record with the extra column
        assertTrue(outEvents.contains(Event.createUpsert(tableRef, modifiedRow)));
    }

    @Test
    public void binlogSync_allBinlogEvents() throws Exception {
        TableRef tableRef = new TableRef("test_schema", "test_table");
        TableDefinition tableDef = new TableDefinition(tableRef, Arrays.asList(new ColumnDefinition("id", "text", true), new ColumnDefinition("data", "text", false)));
        Row insertedRow = new Row("1", "foo-1"), updatedRow = new Row("2", "foo-2");
        AgentState state = new AgentState();
        TableState tableState = new TableState();

        tableDefinitions.put(tableRef, tableDef);
        config.schemas.put("test_schema", new SchemaConfig());
        config.schemas.get("test_schema").selectOtherTables = true;

        tableState.finishedImport = true;
        state.tableStates.put(tableRef, tableState);
        state.tableDefinitions.put(tableRef, tableDef);
        state.binlogPosition = new BinlogPosition("mysql-bin-changelog.000001", 1L);

        SourceEvent insert = SourceEvent.createInsert(tableRef, new BinlogPosition("mysql-bin-changelog.000001", 2L), ImmutableList.of(insertedRow));
        SourceEvent other1 = SourceEvent.createOther(new BinlogPosition("mysql-bin-changelog.000001", 3L));
        SourceEvent update = SourceEvent.createUpdate(tableRef, new BinlogPosition("mysql-bin-changelog.000001", 4L), ImmutableList.of(insertedRow), ImmutableList.of(updatedRow));
        SourceEvent other2 = SourceEvent.createOther(new BinlogPosition("mysql-bin-changelog.000001", 5L));
        SourceEvent delete = SourceEvent.createDelete(tableRef, new BinlogPosition("mysql-bin-changelog.000001", 6L), ImmutableList.of(updatedRow));

        sourceEvents = ImmutableList.of(insert, other1, update, other2, delete);
        sourceEventIterator = sourceEvents.iterator();
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

    // TODO test for what should happen when SchemaConfig selected = false, but selectOtherTables = true
    // TODO maybe play around with this test so that it will import the table after doing a binlog sync
    @Test
    public void update_onlySyncSelectedTablesFoundInBinlog() throws Exception {
        TableRef selectedTable = new TableRef("selected_schema", "selected_table");
        TableRef ignoredTable = new TableRef("selected_schema", "ignored_table");
        TableRef ignoredSchema = new TableRef("ignored_schema", "ignored_table");

        config.schemas.put("selected_schema", new SchemaConfig());
        config.schemas.get("selected_schema").tables.put("selected_table", new TableConfig());
        config.schemas.get("selected_schema").tables.put("ignored_table", new TableConfig());
        config.schemas.get("selected_schema").tables.get("ignored_table").selected = false;

        config.schemas.put("ignored_schema", new SchemaConfig());
        config.schemas.get("ignored_schema").selected = false;
        config.schemas.get("ignored_schema").tables.put("ignored_table", new TableConfig());
        config.schemas.get("ignored_schema").tables.get("ignored_table").selected = false;

        Row row1 = row("1", "foo-1"), row2 = row("2", "foo-2"), row3 = row("3", "foo-3");

        SourceEvent insertIgnoredTable = SourceEvent.createInsert(ignoredTable, new BinlogPosition("mysql-bin-changelog.000001", 2L), ImmutableList.of(row1));
        SourceEvent insertIgnoredSchema = SourceEvent.createInsert(ignoredSchema, new BinlogPosition("mysql-bin-changelog.000001", 3L), ImmutableList.of(row2));
        SourceEvent insertSelectedTable = SourceEvent.createInsert(selectedTable, new BinlogPosition("mysql-bin-changelog.000001", 4L), ImmutableList.of(row3));
        sourceEvents = ImmutableList.of(insertIgnoredTable, insertIgnoredSchema, insertSelectedTable);
        binlogPosition = new BinlogPosition("mysql-bin-changelog.000001", 5L);

        TableDefinition tableDef = new TableDefinition(selectedTable, Arrays.asList(new ColumnDefinition("id", "text", true), new ColumnDefinition("data", "text", false)));

        Supplier<Map<TableRef, TableDefinition>> updatableTableDefinitions = () -> {
            if (outEvents.size() == 0)
                return Collections.emptyMap();
            else
                return Collections.singletonMap(selectedTable, tableDef);
        };

        MysqlApi customApi = new MysqlApi(importTable, null, null, updatableTableDefinitions, read);

        new BatchUpdater(config, customApi, out, logMessages::add, state).update();

        assertEquals(outEvents.size(), 6);

        // First binlog sync
        assertTrue(outEvents.contains(Event.createNop()));  // insertIgnoredTable
        assertTrue(outEvents.contains(Event.createNop()));  // insertIgnoredSchema

        // Second binlog sync
        assertTrue(outEvents.contains(Event.createNop()));  // insertIgnoredTable
        assertTrue(outEvents.contains(Event.createNop()));  // insertIgnoredSchema
        assertTrue(outEvents.contains(Event.createUpsert(selectedTable, row3)));  // insertSelectedTable
        assertTrue(outEvents.contains(Event.createNop()));  // TIMEOUT
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
        state.tableStates.put(tableRef, tableState);

        TableDefinition selectedTableDef = new TableDefinition(tableRef, Arrays.asList(new ColumnDefinition("id", "text", true), new ColumnDefinition("data", "text", false)));
        tableDefinitions.put(tableRef, selectedTableDef);

        Row row1 = row("1", "foo-1"), row2 = row("2", "foo-2"), row3 = row("3", "foo-3"), row4 = row("4", "I pity the foo");
        readRows = ImmutableList.of(row1, row2, row3, row4);

        BatchUpdater updater = new BatchUpdater(config, api, out, logMessages::add, state);

        updater.update();
    }

    private Row row(String... values) {
        Row row = new Row(values.length);
        row.addAll(Arrays.asList(values));
        return row;
    }
}
