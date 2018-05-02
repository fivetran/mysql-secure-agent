package com.fivetran.agent.mysql;

import com.fivetran.agent.mysql.config.Config;
import com.fivetran.agent.mysql.log.Logger;
import com.fivetran.agent.mysql.log.RealLogWriter;
import com.fivetran.agent.mysql.output.*;
import com.fivetran.agent.mysql.source.*;
import com.fivetran.agent.mysql.source.binlog.client.EventReader;
import com.fivetran.agent.mysql.state.AgentState;
import com.google.common.collect.ImmutableList;
import org.junit.Test;

import java.util.*;

public class IntegrationWithMockSourceSpec {
    private List<Row> readRows = new ArrayList<>();

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

    private BinlogPosition binlogPosition = new BinlogPosition("file", -1L);
    private final Output out = new BucketOutput(new S3Client("mysql-secure-agent-test"), 64);
    private final Log log = new Logger(new RealLogWriter());

    @Test
    public void writeToS3UsingMockedSourceData() throws Exception {
        MysqlApi api = new MysqlApi(importTable, null, null, () -> tableDefinitions, read);
        Updater updater = new Updater(new Config(), api, out, log, new AgentState());

        TableRef tableRef = new TableRef("test_schema", "test_table");
        TableDefinition selectedTableDef = new TableDefinition(tableRef, Arrays.asList(new ColumnDefinition("id", "text", true), new ColumnDefinition("data", "text", false)));
        tableDefinitions.put(tableRef, selectedTableDef);

        Row row1 = new Row("1", "foo-1"), row2 = new Row("2", "foo-2"), row3 = new Row("3", "foo-3"), row4 = new Row("4", "foo-4");


        readRows = ImmutableList.of(row1, row2);

        updater.update();
    }
}
