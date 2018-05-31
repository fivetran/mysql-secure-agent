/**
* Copyright (c) Fivetran 2018
**/
package com.fivetran.agent.mysql;

import com.fivetran.agent.mysql.config.Config;
import com.fivetran.agent.mysql.config.DatabaseCredentials;
import com.fivetran.agent.mysql.log.LogGeneralException;
import com.fivetran.agent.mysql.output.BucketOutput;
import com.fivetran.agent.mysql.output.S3Client;
import com.fivetran.agent.mysql.source.*;
import com.fivetran.agent.mysql.source.binlog.client.EventReader;
import com.fivetran.agent.mysql.state.AgentState;
import org.junit.Test;

import java.util.Optional;

import static com.fivetran.agent.mysql.Main.LOG;

public class IntegrationWithRealSourceTest {

    private BinlogPosition binlogPosition = new BinlogPosition("file", -1L);
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
                    return null;
                }

                @Override
                public void close() {

                }
            };
        }
    };

    @Test
    public void writeToS3WithRealSource() {
        DatabaseCredentials creds = new DatabaseCredentials("", 3306, "", "");
        Query query = new QueryDatabase(creds);
        ImportTable importTable = new ImportSourceTable(query);
        TableDefinitions tableDefinitions = new TableDefinitions(query);

        MysqlApi api = new MysqlApi(importTable, null, null, tableDefinitions, read);

        try (Output out = new BucketOutput(new S3Client("mysql-secure-agent-test"), 1024 * 256)) {
            Updater updater = new Updater(new Config(), api, out, LOG, new AgentState());
            updater.update();
        } catch (Exception e) {
            LOG.log(new LogGeneralException(e));
        }
    }
}
