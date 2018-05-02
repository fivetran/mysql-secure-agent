/**
* Copyright (c) Fivetran 2018
**/
package com.fivetran.agent.mysql.log;

import com.fasterxml.jackson.databind.JsonNode;
import com.fivetran.agent.mysql.Main;
import com.fivetran.agent.mysql.source.TableRef;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertTrue;

public class LoggerSpec {
    private List<String> logMessages = new ArrayList<>();
    private Logger logger = new Logger((byteBuffer -> logMessages.add(new String(byteBuffer.array()))));

    @Test
    public void log_beginSelectPage() throws IOException {
        TableRef tableRef = new TableRef("test_schema", "test_table");
        LogMessage message = new BeginSelectPage(tableRef, Optional.of(Collections.singletonList("123")));
        logger.log(message);

        JsonNode topLevel = Main.JSON.readValue(logMessages.get(0), JsonNode.class);
        assertThat(topLevel.get("level").asText(), equalTo("INFO"));
        assertThat(topLevel.get("event").asText(), equalTo("BEGIN_SELECT_PAGE"));

        JsonNode details = topLevel.get("details");
        assertThat(details.get("fromKey").get(0).asText(), equalTo("123"));
        assertThat(details.get("table").get("schemaName").asText(), equalTo("test_schema"));
        assertThat(details.get("table").get("tableName").asText(), equalTo("test_table"));

        assertThat(logMessages.size(), equalTo(1));
    }

    @Test
    public void log_generalException() throws IOException {
        LogMessage message = new LogGeneralException(new RuntimeException("Something went super duper wrong!"));
        logger.log(message);

        JsonNode topLevel = Main.JSON.readValue(logMessages.get(0), JsonNode.class);
        assertThat(topLevel.get("level").asText(), equalTo("ERROR"));
        assertThat(topLevel.get("event").asText(), equalTo("LOG_GENERAL_EXCEPTION"));
        assertThat(topLevel.get("details").get("message").asText(), equalTo("Something went super duper wrong!"));

        assertThat(logMessages.size(), equalTo(1));
    }

    @Test
    public void log_beginReadBinlog() throws IOException {
        LogMessage message = new BeginReadBinlog();
        logger.log(message);

        JsonNode topLevel = Main.JSON.readValue(logMessages.get(0), JsonNode.class);
        assertThat(topLevel.get("level").asText(), equalTo("INFO"));
        assertThat(topLevel.get("event").asText(), equalTo("BEGIN_READ_BINLOG"));

        assertThat(logMessages.size(), equalTo(1));
    }
}
