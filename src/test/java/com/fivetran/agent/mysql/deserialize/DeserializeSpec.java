/**
* Copyright (c) Fivetran 2018
**/
package com.fivetran.agent.mysql.deserialize;

import com.fivetran.agent.mysql.config.ColumnConfig;
import com.fivetran.agent.mysql.config.Config;
import com.fivetran.agent.mysql.config.SchemaConfig;
import com.fivetran.agent.mysql.config.TableConfig;
import com.fivetran.agent.mysql.credentials.Credentials;
import com.fivetran.agent.mysql.source.TableRef;
import com.fivetran.agent.mysql.state.AgentState;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.util.Optional;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class DeserializeSpec {

    @Test
    public void deserializeCredentials() {
        String credentialsJson = "{\n" +
                "  \"s3Credentials\": {\n" +
                "    \"bucket\": \"bucket\"\n" +
                "  },\n" +
                "  \"dbCredentials\": {\n" +
                "    \"host\": \"database.com\",\n" +
                "    \"port\": 1234,\n" +
                "    \"user\": \"developers\",\n" +
                "    \"password\": \"password\"\n" +
                "  }\n" +
                "}";

        Credentials credentials = Deserialize.deserialize(new ByteArrayInputStream(credentialsJson.getBytes()), Credentials.class);
        assertThat(credentials.s3Credentials.bucket, equalTo("bucket"));
        assertThat(credentials.dbCredentials.host, equalTo("database.com"));
        assertThat(credentials.dbCredentials.port, equalTo(1234));
        assertThat(credentials.dbCredentials.user, equalTo("developers"));
        assertThat(credentials.dbCredentials.password, equalTo("password"));

        System.getenv();
    }

    @Test
    public void deserializeEmptyCredentials() {
        Credentials credentials = Deserialize.deserialize(new ByteArrayInputStream("".getBytes()), Credentials.class);
        assertThat(credentials, equalTo(new Credentials()));
    }

    @Test
    public void deserializeState() {
        String stateJson = "{\n" +
                "  \"binlogPosition\": {\n" +
                "    \"file\": \"some_binlog_file\",\n" +
                "    \"position\": 1234567890\n" +
                "  },\n" +
                "  \"tables\": {\n" +
                "    \"schema_one.table_one\": {\n" +
                "      \"finishedImport\": true,\n" +
                "      \"lastSyncedPrimaryKey\": {\n" +
                "        \"table_pkey\": \"pkey_value\"\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}";
        AgentState state = Deserialize.deserialize(new ByteArrayInputStream(stateJson.getBytes()), AgentState.class);
        assertThat(state.tables.get(new TableRef("schema_one", "table_one")).finishedImport, equalTo(true));
        assertThat(state.tables.get(new TableRef("schema_one", "table_one")).lastSyncedPrimaryKey.isPresent(), equalTo(true));
        assertThat(state.tables.get(new TableRef("schema_one", "table_one")).lastSyncedPrimaryKey.get().get("table_pkey"), equalTo("pkey_value"));
        assertThat(state.binlogPosition.file, equalTo("some_binlog_file"));
        assertThat(state.binlogPosition.position, equalTo(1234567890L));
    }

    @Test
    public void deserializeEmptyState() {
        AgentState state = Deserialize.deserialize(new ByteArrayInputStream("".getBytes()), AgentState.class);
        assertThat(state, equalTo(new AgentState()));
    }

    @Test
    public void deserializeConfig() {
        String configString = "{\n" +
                "  \"schemas\": {\n" +
                "    \"schema_one\": {\n" +
                "      \"selected\": false,\n" +
                "      \"selectOtherTables\": false," +
                "      \"tables\": {\n" +
                "        \"table_one\": {\n" +
                "          \"selected\": false,\n" +
                "          \"selectOtherColumns\": false,\n" +
                "          \"columns\": {\n" +
                "            \"column_one\": {\n" +
                "              \"selected\": false,\n" +
                "              \"hash\": true,\n" +
                "              \"implicitKey\": true\n" +
                "            }\n" +
                "          }\n" +
                "        }\n" +
                "      }\n" +
                "    }\n" +
                "  },\n" +
                "  \"selectOtherSchemas\": false,\n" +
                "  \"cryptoSalt\": \"salzig\"\n" +
                "}";
        Config config = Deserialize.deserialize(new ByteArrayInputStream(configString.getBytes()), Config.class);
        assertThat(config.cryptoSalt, equalTo("salzig"));
        assertThat(config.selectOtherSchemas, equalTo(false));

        SchemaConfig schemaOne = config.schemas.get("schema_one");
        assertThat(schemaOne.selectOtherTables, equalTo(false));
        assertThat(schemaOne.selected, equalTo(false));

        TableConfig tableOne = schemaOne.tables.get("table_one");
        assertThat(tableOne.selectOtherColumns, equalTo(false));
        assertThat(tableOne.selected, equalTo(false));

        ColumnConfig columnOne = tableOne.columns.get("column_one");
        assertThat(columnOne.hash, equalTo(true));
        assertThat(columnOne.implicitKey, equalTo(Optional.of(true)));
        assertThat(columnOne.selected, equalTo(false));
    }

    @Test
    public void deserializeEmptyConfig() {
        Config config = Deserialize.deserialize(new ByteArrayInputStream("".getBytes()), Config.class);
        assertThat(config, equalTo(new Config()));
    }
}
