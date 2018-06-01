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
import com.fivetran.agent.mysql.source.binlog.client.BinlogClient;
import com.fivetran.agent.mysql.state.AgentState;
import org.junit.Ignore;
import org.junit.Test;

import static com.fivetran.agent.mysql.Main.LOG;

@Ignore
public class IntegrationWithRealSourceTest {
    @Test
    public void writeToS3WithRealSource() {
        DatabaseCredentials creds = new DatabaseCredentials("localhost", 3306, "", "");
        Query query = new QueryDatabase(creds);
        ImportTable importTable = new ImportSourceTable(query);
        TableDefinitions tableDefinitions = new TableDefinitions(query);
        BinlogClient client = new BinlogClient(creds);

        MysqlApi api = new MysqlApi(importTable, null, null, tableDefinitions, client);

        try (Output out = new BucketOutput(new S3Client("mysql-secure-agent-test"))) {
            BatchUpdater updater = new BatchUpdater(new Config(), api, out, LOG, new AgentState());
            updater.update();
        } catch (Exception e) {
            LOG.log(new LogGeneralException(e));
        }
    }
}
