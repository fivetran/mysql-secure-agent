/**
 * Copyright (c) Fivetran 2018
 **/
package com.fivetran.agent.mysql.deserialize;

import com.fivetran.agent.mysql.config.ColumnConfig;
import com.fivetran.agent.mysql.config.Config;
import com.fivetran.agent.mysql.config.SchemaConfig;
import com.fivetran.agent.mysql.config.TableConfig;
import com.fivetran.agent.mysql.credentials.Credentials;
import com.fivetran.agent.mysql.output.ColumnDefinition;
import com.fivetran.agent.mysql.output.ForeignKey;
import com.fivetran.agent.mysql.source.TableRef;
import com.fivetran.agent.mysql.state.AgentState;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.util.Optional;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.*;

public class DeserializeTest {

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
                "  \"binlogPosition\" : {\n" +
                "    \"file\" : \"some_binlog_file\",\n" +
                "    \"position\" : 1234567890\n" +
                "  },\n" +
                "  \"tableStates\" : {\n" +
                "    \"primary_schema.primary_table\" : {\n" +
                "      \"lastSyncedPrimaryKey\" : {\n" +
                "        \"primary_pkey\" : \"primary_pkey_value\"\n" +
                "      },\n" +
                "      \"finishedImport\" : true\n" +
                "    },\n" +
                "    \"foreign_schema.foreign_table\" : {\n" +
                "      \"lastSyncedPrimaryKey\" : null,\n" +
                "      \"finishedImport\" : false\n" +
                "    }\n" +
                "  },\n" +
                "  \"tableDefinitions\" : {\n" +
                "    \"primary_schema.primary_table\" : {\n" +
                "      \"table\" : \"primary_schema.primary_table\",\n" +
                "      \"foreignKeys\" : {\n" +
                "        \"foreign_schema.foreign_table\" : {\n" +
                "          \"columns\" : [ \"primary_fkey\" ],\n" +
                "          \"referencedColumns\" : [ \"foreign_pkey\" ]\n" +
                "        }\n" +
                "      },\n" +
                "      \"tableDefinition\" : [ {\n" +
                "        \"name\" : \"primary_pkey\",\n" +
                "        \"type\" : \"text\",\n" +
                "        \"key\" : true\n" +
                "      }, {\n" +
                "        \"name\" : \"primary_fkey\",\n" +
                "        \"type\" : \"text\",\n" +
                "        \"key\" : false\n" +
                "      } ]\n" +
                "    },\n" +
                "    \"foreign_schema.foreign_table\" : {\n" +
                "      \"table\" : \"foreign_schema.foreign_table\",\n" +
                "      \"foreignKeys\" : null,\n" +
                "      \"tableDefinition\" : [ {\n" +
                "        \"name\" : \"foreign_pkey\",\n" +
                "        \"type\" : \"text\",\n" +
                "        \"key\" : true\n" +
                "      } ]\n" +
                "    }\n" +
                "  }\n" +
                "}";

        TableRef primaryTableRef = new TableRef("primary_schema", "primary_table");
        TableRef foreignTableRef = new TableRef("foreign_schema", "foreign_table");

        AgentState state = Deserialize.deserialize(new ByteArrayInputStream(stateJson.getBytes()), AgentState.class);

        assertTrue(state.tableStates.get(primaryTableRef).finishedImport);
        assertThat(state.tableStates.get(primaryTableRef).lastSyncedPrimaryKey.get().get("primary_pkey"), equalTo("primary_pkey_value"));

        assertFalse(state.tableStates.get(foreignTableRef).finishedImport);
        assertFalse(state.tableStates.get(foreignTableRef).lastSyncedPrimaryKey.isPresent());

        assertThat(state.tableDefinitions.get(primaryTableRef).table, equalTo(primaryTableRef));
        assertThat(state.tableDefinitions.get(primaryTableRef).columns.size(), equalTo(2));
        assertThat(state.tableDefinitions.get(primaryTableRef).columns.get(0), equalTo(new ColumnDefinition("primary_pkey", "text", true)));
        assertThat(state.tableDefinitions.get(primaryTableRef).columns.get(1), equalTo(new ColumnDefinition("primary_fkey", "text", false)));
        assertThat(state.tableDefinitions.get(primaryTableRef).foreignKeys.get(foreignTableRef), equalTo(new ForeignKey("primary_fkey", "foreign_pkey")));

        assertThat(state.tableDefinitions.get(foreignTableRef).table, equalTo(foreignTableRef));
        assertThat(state.tableDefinitions.get(foreignTableRef).columns.size(), equalTo(1));
        assertThat(state.tableDefinitions.get(foreignTableRef).columns.get(0), equalTo(new ColumnDefinition("foreign_pkey", "text", true)));
        assertThat(state.tableDefinitions.get(foreignTableRef).foreignKeys, equalTo(null));

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
