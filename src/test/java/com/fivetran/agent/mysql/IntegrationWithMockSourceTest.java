/**
* Copyright (c) Fivetran 2018
**/
package com.fivetran.agent.mysql;

import com.fivetran.agent.mysql.config.Config;
import com.fivetran.agent.mysql.log.Logger;
import com.fivetran.agent.mysql.log.RealLogWriter;
import com.fivetran.agent.mysql.output.*;
import com.fivetran.agent.mysql.source.*;
import com.fivetran.agent.mysql.source.binlog.client.EventReader;
import com.fivetran.agent.mysql.state.AgentState;
import com.google.common.collect.ImmutableList;
import org.junit.Ignore;
import org.junit.Test;

import java.util.*;

@Ignore
public class IntegrationWithMockSourceTest {

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

    private ImportTable importTable = (table, selectColumns, pagingParams) -> rows;
    private Map<TableRef, TableDefinition> tableDefinitions = new HashMap<>();
    private BinlogPosition binlogPosition = new BinlogPosition("mysql-bin-changelog.000001", 1L);

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

    private final Log log = new Logger(new RealLogWriter());

    @Test
    public void writeToS3UsingMockedSourceData() throws Exception {
        try (Output out = new BucketOutput(new S3Client("mysql-secure-agent-test"))) {
            MysqlApi api = new MysqlApi(importTable, null, null, () -> tableDefinitions, read);
            BatchUpdater updater = new BatchUpdater(new Config(), api, out, log, new AgentState());

            TableRef tableRef = new TableRef("test_schema", "test_table");
            TableDefinition selectedTableDef = new TableDefinition(tableRef, Arrays.asList(new ColumnDefinition("id", "text", true), new ColumnDefinition("data", "text", false)));
            tableDefinitions.put(tableRef, selectedTableDef);

            readRows = ImmutableList.of(new Row("1", "foo-1"), new Row("2", "foo-2"), new Row("3", "foo-3"), new Row("4", "foo-4"));
            SourceEvent insertEvent = SourceEvent.createInsert(tableRef, new BinlogPosition("mysql-bin-changelog.000001", 2L), ImmutableList.of(new Row("5", "foo-5")));
            sourceEvents.add(insertEvent);

            updater.update();
        }
    }
}
