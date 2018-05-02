package com.fivetran.agent.mysql;

import com.fivetran.agent.mysql.config.Config;
import com.fivetran.agent.mysql.config.SchemaConfig;
import com.fivetran.agent.mysql.config.TableConfig;
import com.fivetran.agent.mysql.log.LogMessage;
import com.fivetran.agent.mysql.output.ColumnDefinition;
import com.fivetran.agent.mysql.output.Emit;
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
import org.junit.Test;

import java.util.*;

import static com.fivetran.agent.mysql.source.SourceEventType.INSERT;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.*;

public class UpdaterSpec {
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
    private BinlogPosition binlogPosition = new BinlogPosition("file", -1L);
    private Map<TableRef, TableDefinition> tableDefinitions = new HashMap<>();
    private AgentState state = new AgentState();
    private ReadSourceLog read = new ReadSourceLog() {
        @Override
        public BinlogPosition currentPosition() {
            return binlogPosition;
        }

        @Override
        public EventReader events(BinlogPosition position) {
            return new EventReader() {
                @Override
                public SourceEvent readEvent() {
                    return null;
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

        Updater updater = new Updater(config, api, out, logMessages::add, state);


        Row row1 = row("1", "foo-1"), row2 = row("2", "foo-2");
        readRows = ImmutableList.of(row1, row2);

        tableDefinitions.clear();
        TableDefinition selectedTableDef = new TableDefinition(selectedTable, Arrays.asList(new ColumnDefinition("id", "text", true), new ColumnDefinition("data", "text", false)));
        tableDefinitions.put(selectedTable, selectedTableDef);
        TableDefinition ignoredTableDef = new TableDefinition(ignoredTable, Arrays.asList(new ColumnDefinition("id", "text", true), new ColumnDefinition("data", "text", false)));
        tableDefinitions.put(ignoredTable, ignoredTableDef);

        updater.update();

        assertEquals(outEvents.size(), 3);
        assertThat(outEvents, not(contains(Emit.row(Event.createUpsert(ignoredTable, row1)))));
        assertThat(outEvents, not(contains(Emit.row(Event.createUpsert(ignoredTable, row2)))));
        assertThat(outEvents, not(contains(Emit.tableDefinition(ignoredTableDef))));
    }

    @Test
    public void update_fullSelectSyncThenBinlog() throws Exception {
        TableRef tableRef = new TableRef("test_schema", "test_table");
        tableDefinitions.clear();
        TableDefinition selectedTableDef = new TableDefinition(tableRef, Arrays.asList(new ColumnDefinition("id", "text", true), new ColumnDefinition("data", "text", false)));
        tableDefinitions.put(tableRef, selectedTableDef);

        Updater updater = new Updater(config, api, out, logMessages::add, state);

        Row row1 = row("1", "foo-1"), row2 = row("2", "foo-2"), row3 = row("3", "foo-3"), row4 = row("4", "foo-4");

        readRows = ImmutableList.of(row1, row2);

        updater.update();

        assertThat(outEvents.size(), equalTo(3));
        assertTrue(outEvents.contains(Event.createUpsert(tableRef, row1)));
        assertTrue(outEvents.contains(Event.createUpsert(tableRef, row2)));
        assertTrue(outEvents.contains(Event.createTableDefinition(selectedTableDef)));

        SourceEvent event1 = new SourceEvent(tableRef, new BinlogPosition("file1", 0), INSERT, ImmutableList.of(row3));
        SourceEvent event2 = new SourceEvent(tableRef, new BinlogPosition("file1", 1), INSERT, ImmutableList.of(row4));
        binlogPosition = new BinlogPosition("file1", 1);
        sourceEvents = ImmutableList.of(event1, event2);

        updater.update();

        assertThat(outEvents.size(), equalTo(6));
        assertTrue(outEvents.contains(Event.createUpsert(tableRef, row3)));
        assertTrue(outEvents.contains(Event.createUpsert(tableRef, row4)));
        // The table definition is included twice
        assertTrue(outEvents.contains(Event.createTableDefinition(selectedTableDef)));
    }

//    @Test
//    public void update_withExistingSelectState() {
//        TableRef tableRef = new TableRef("test_schema", "test_table");
//
//        AgentState state = new AgentState();
//        TableState tableState = new TableState();
//        tableState.lastSyncedPrimaryKey = Optional.of(Collections.singletonMap("id", "3"));
//        state.tables.put(tableRef, tableState);
//        tableDefinitions.clear();
//        TableDefinition selectedTableDef = new TableDefinition(tableRef, Arrays.asList(new ColumnDefinition("id", "text", true), new ColumnDefinition("data", "text", false)));
//        tableDefinitions.put(tableRef, selectedTableDef);
//
//        Row row1 = row("1", "foo-1"), row2 = row("2", "foo-2"), row3 = row("3", "foo-3"), row4 = row("4", "I pity the foo");
//        readRows = ImmutableList.of(row1, row2, row3, row4);
//
//        Updater updater = new Updater(config, api, out, logMessages::add, state);
//
//        updater.update();
//
//        System.out.println();
//    }
//
//    // TODO turns out incremental updates aren't working. Who knew? Oh, right because it depends on the API part to serve up the right rows and we just wanna test the query
//
//    // TODO how do we incrementally update hashed keys?
//
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

        Updater updater = new Updater(config, api, out, logMessages::add, state);
        updater.update();

        assertFalse(outEvents.contains(Event.createUpsert(tableRef, Arrays.asList("1", "foo-1"))));
        assertTrue(outEvents.contains(Event.createUpsert(tableRef, Arrays.asList("AnZXUjEr5i2a57kXUtI6dXftv+E=", "foo-1"))));
        assertTrue(outEvents.contains(Event.createUpsert(tableRef, Arrays.asList("jUdn9gOx372QFiYR9zRyZp0VQEw=", "foo-2"))));
    }

    @Test
    public void selectSync() {
        TableRef tableRef = new TableRef("test_schema", "sync_table");
        AgentState state = new AgentState();
        state.tables.put(tableRef, new TableState());

        Updater updater = new Updater(config, api, out, logMessages::add, state);

        Row row1 = row("1", "foo-1"), row2 = row("2", "foo-2");
        readRows = ImmutableList.of(row1, row2);

        tableDefinitions.clear();
        TableDefinition tableDef = new TableDefinition(tableRef, Arrays.asList(new ColumnDefinition("id", "text", true), new ColumnDefinition("data", "text", false)));
        tableDefinitions.put(tableRef, tableDef);

        updater.syncPageFromTable(tableDef);

        assertTrue(outEvents.contains(Event.createTableDefinition(tableDef)));

        assertTrue(outEvents.contains(Event.createUpsert(tableRef, Arrays.asList("1", "foo-1"))));
        assertTrue(outEvents.contains(Event.createUpsert(tableRef, Arrays.asList("2", "foo-2"))));
    }

    @Test
    public void binlogSync_allInserts() throws Exception {
        TableRef tableRef = new TableRef("test_schema", "binlog_sync");
        AgentState state = new AgentState();
        state.binlogPosition = new BinlogPosition("current_file", 1L);
        TableState tableState = new TableState();
        tableState.finishedImport = true;
        state.tables.put(tableRef, tableState);

        tableDefinitions.clear();
        TableDefinition tableDef = new TableDefinition(tableRef, Arrays.asList(new ColumnDefinition("id", "text", true), new ColumnDefinition("data", "text", false)));
        tableDefinitions.put(tableRef, tableDef);

        Row row1 = new Row("1", "foo-1"), row2 = new Row("2", "foo-2");

        SourceEvent event1 = new SourceEvent(tableRef, new BinlogPosition("file1", 0), INSERT, Arrays.asList(row1));
        SourceEvent event2 = new SourceEvent(tableRef, new BinlogPosition("file1", 1), INSERT, Arrays.asList(row2));

        sourceEvents = ImmutableList.of(event1, event2);
        Updater updater = new Updater(config, api, out, logMessages::add, state);
        updater.update();

        assertEquals(outEvents.size(), 3);

        assertTrue(outEvents.contains(Event.createTableDefinition(tableDef)));

        assertTrue(outEvents.contains(Event.createUpsert(tableRef, Arrays.asList("1", "foo-1"))));
        assertTrue(outEvents.contains(Event.createUpsert(tableRef, Arrays.asList("2", "foo-2"))));
    }

    private Row row(String... values) {
        Row row = new Row(values.length);
        row.addAll(Arrays.asList(values));
        return row;
    }
}
