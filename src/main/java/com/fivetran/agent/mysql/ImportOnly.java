package com.fivetran.agent.mysql;

import com.fivetran.agent.mysql.config.Config;
import com.fivetran.agent.mysql.config.DatabaseCredentials;
import com.fivetran.agent.mysql.log.LogGeneralException;
import com.fivetran.agent.mysql.output.BucketOutput;
import com.fivetran.agent.mysql.output.S3Client;
import com.fivetran.agent.mysql.source.*;
import com.fivetran.agent.mysql.source.binlog.client.EventReader;
import com.fivetran.agent.mysql.state.AgentState;

import java.util.Optional;

import static com.fivetran.agent.mysql.Main.LOG;

public class ImportOnly {

    public static void main(String args[]) {

        ReadSourceLog read = new ReadSourceLog() {
            @Override
            public BinlogPosition currentPosition() {
                return new BinlogPosition("file", -1L);
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

        DatabaseCredentials creds = new DatabaseCredentials("", 3306, "", "");
        Query query = new QueryDatabase(creds);
        ImportTable importTable = new ImportSourceTable(query);
        TableDefinitions tableDefinitions = new TableDefinitions(query);
        CleanupStaleSessions cleanup = new CleanupStaleSessions();

        MysqlApi api = new MysqlApi(importTable, cleanup, null, tableDefinitions, read);

        try (Output out = new BucketOutput(new S3Client(""), 128)) {
            Updater updater = new Updater(new Config(), api, out, LOG, new AgentState());
            updater.update();
        } catch (Exception e) {
            LOG.log(new LogGeneralException(e));
        }
    }
}
