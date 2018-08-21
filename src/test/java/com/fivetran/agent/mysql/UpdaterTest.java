/**
* Copyright (c) Fivetran 2018
**/
package com.fivetran.agent.mysql;

import com.fivetran.agent.mysql.config.Config;
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
import com.google.common.collect.Sets;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.*;

// TODO since Updater#update will infinitely loop, we can use BatchUpdaterSpec for integration tests. We'll write what unit tests we can here
public class UpdaterTest {
    private Config config = new Config();
    private List<Row> readRows = new ArrayList<>();
    private List<SourceEvent> sourceEvents = new ArrayList<>();
    private Rows rows = new Rows() {
        @Override
        public List<String> columnNames() {
            return null;
        }

        @Override
        public void close() {}

        @Override
        public Iterator<Row> iterator() {
            return readRows.iterator();
        }
    };
    private BinlogPosition binlogPosition;
    private Map<TableRef, TableDefinition> tableDefinitions = new HashMap<>();
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

    @Before
    public void before() {
        tableDefinitions.clear();
    }

    @Test
    public void selectSync() {
        TableRef tableRef = new TableRef("schema", "sync_table");
        AgentState state = new AgentState();
        Updater updater = new Updater(config, api, out, logMessages::add, state);

        Row row1 = row("1", "foo-1"), row2 = row("2", "foo-2");
        readRows = ImmutableList.of(row1, row2);

        TableDefinition tableDef = new TableDefinition(tableRef, Arrays.asList(new ColumnDefinition("id", "text", true), new ColumnDefinition("data", "text", false)));
        tableDefinitions.put(tableRef, tableDef);

        updater.updateState();
        updater.syncPageFromTable(tableRef);

        assertTrue(outEvents.contains(Event.createUpsert(tableRef, Arrays.asList("1", "foo-1"))));
        assertTrue(outEvents.contains(Event.createUpsert(tableRef, Arrays.asList("2", "foo-2"))));
    }

    @Test
    public void updateStateTableDefinintions() {
        TableRef tableRef = new TableRef("schema", "table");
        AgentState state = new AgentState();
        Updater updater = new Updater(config, api, out, logMessages::add, state);

        TableDefinition initialTableDef = new TableDefinition(tableRef, Collections.singletonList(new ColumnDefinition("initial", "text", true)));
        tableDefinitions.put(tableRef, initialTableDef);

        updater.updateState();

        assertTrue(state.tableDefinitions.size() == 1);
        assertTrue(state.tableDefinitions.get(tableRef).equals(initialTableDef));

        TableDefinition updatedTableDef = new TableDefinition(tableRef, Collections.singletonList(new ColumnDefinition("updated", "int", true)));
        tableDefinitions.put(tableRef, updatedTableDef);

        updater.updateState();

        assertTrue(state.tableDefinitions.size() == 1);
        assertTrue(state.tableDefinitions.get(tableRef).equals(updatedTableDef));
    }

    @Test
    public void updateEmptyState() {
        AgentState state = new AgentState();
        Updater updater = new Updater(config, api, out, logMessages::add, state);

        TableRef tableRef = new TableRef("schema", "table");
        TableDefinition tableDef = new TableDefinition(tableRef);

        tableDefinitions.put(tableRef, tableDef);
        binlogPosition = new BinlogPosition("mysql-bin-changelog.000001", 1L);

        updater.updateState();

        assertTrue(state.tableStates.size() == 1);
        assertTrue(state.tableDefinitions.size() == 1);
        assertTrue(state.tableStates.get(tableRef).equals(new TableState()));
        assertTrue(state.tableDefinitions.get(tableRef).equals(tableDef));
        assertTrue(state.binlogPosition.equals(binlogPosition));
    }

    @Test
    public void updateNonEmptyState() {
        AgentState state = new AgentState();
        Updater updater = new Updater(config, api, out, logMessages::add, state);

        TableRef existingTableRef = new TableRef("existing_schema", "existing_table");
        TableDefinition existingTableDef = new TableDefinition(existingTableRef);
        BinlogPosition shouldBeInState = new BinlogPosition("should_be_in_state", 1L);

        tableDefinitions.put(existingTableRef, existingTableDef);
        binlogPosition = shouldBeInState;

        updater.updateState();

        assertTrue(state.tableStates.size() == 1);
        assertTrue(state.tableDefinitions.size() == 1);
        assertTrue(state.tableStates.get(existingTableRef).equals(new TableState()));
        assertTrue(state.tableDefinitions.get(existingTableRef).equals(existingTableDef));
        assertTrue(state.binlogPosition.equals(binlogPosition));

        TableRef newTableRef = new TableRef("new_schema", "new_table");
        TableDefinition newTableDef = new TableDefinition(newTableRef);
        BinlogPosition shouldNotBeInState = new BinlogPosition("should_not_be_in_state", 1L);

        tableDefinitions.put(newTableRef, newTableDef);
        binlogPosition = shouldNotBeInState;

        updater.updateState();

        assertTrue(state.tableStates.size() == 2);
        assertTrue(state.tableDefinitions.size() == 2);
        assertTrue(state.tableStates.get(existingTableRef).equals(new TableState()));
        assertTrue(state.tableStates.get(newTableRef).equals(new TableState()));
        assertTrue(state.tableDefinitions.get(existingTableRef).equals(existingTableDef));
        assertTrue(state.tableDefinitions.get(newTableRef).equals(newTableDef));
        assertTrue(state.binlogPosition.equals(shouldBeInState));
    }

    @Test
    public void removeLostTablesFromState() {
        TableRef tableRef = new TableRef("schema", "sync_table");
        TableRef lostTable = new TableRef("schema", "lost_table");
        AgentState state = new AgentState();

        state.tableStates.put(lostTable, new TableState());


        Updater updater = new Updater(config, api, out, logMessages::add, state);

        Row row1 = row("1", "foo-1"), row2 = row("2", "foo-2");
        readRows = ImmutableList.of(row1, row2);

        TableDefinition tableDef = new TableDefinition(tableRef, Arrays.asList(new ColumnDefinition("id", "text", true), new ColumnDefinition("data", "text", false)));
        tableDefinitions.put(tableRef, tableDef);

        updater.updateState();

        assertTrue(state.tableStates.size() == 1);
        assertTrue(state.tableStates.containsKey(tableRef));
    }

    private Row row(String... values) {
        Row row = new Row(values.length);
        row.addAll(Arrays.asList(values));
        return row;
    }
}
