package com.fivetran.agent.mysql;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fivetran.agent.mysql.config.Config;
import com.fivetran.agent.mysql.credentials.Credentials;
import com.fivetran.agent.mysql.deserialize.Deserialize;
import com.fivetran.agent.mysql.log.LogGeneralException;
import com.fivetran.agent.mysql.log.Logger;
import com.fivetran.agent.mysql.log.RealLogWriter;
import com.fivetran.agent.mysql.output.BucketOutput;
import com.fivetran.agent.mysql.output.S3Client;
import com.fivetran.agent.mysql.source.*;
import com.fivetran.agent.mysql.source.binlog.client.BinlogClient;

public class Main {
    public static final Log LOG = new Logger(new RealLogWriter());

    public static final ObjectMapper JSON = new ObjectMapper()
            .registerModule(new Jdk8Module())
            .configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);

    public static void main (String args[]) {
        Credentials creds = Deserialize.credentials();

        Query query = new QueryDatabase(creds.dbCredentials);
        ImportTable importTable = new ImportSourceTable(query);
        TableDefinitions tableDefinitions = new TableDefinitions(query);
        BinlogClient client = new BinlogClient(creds.dbCredentials);
        CleanupStaleSessions cleanup = new CleanupStaleSessions();

        MysqlApi api = new MysqlApi(importTable, cleanup, null, tableDefinitions, client);

        try (Output out = new BucketOutput(new S3Client(creds.s3Credentials.bucket))) {
            Updater updater = new Updater(new Config(), api, out, LOG, Deserialize.state());
            updater.update();
        } catch (Throwable e) {
            LOG.log(new LogGeneralException(e));
        }
    }
}
