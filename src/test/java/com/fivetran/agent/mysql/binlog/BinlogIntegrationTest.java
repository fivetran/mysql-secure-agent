/**
* Copyright (c) Fivetran 2018
**/
package com.fivetran.agent.mysql.binlog;

import com.fivetran.agent.mysql.config.DatabaseCredentials;
import com.fivetran.agent.mysql.source.*;
import com.fivetran.agent.mysql.source.binlog.client.BinlogClient;
import com.fivetran.agent.mysql.source.binlog.client.EventReader;
import org.junit.*;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

@Ignore
public class BinlogIntegrationTest {
    private static DatabaseCredentials creds;
    private static DataSource dataSource;
    private static String database = "mysql_agent_binlog_integration";

    @BeforeClass
    public static void beforeClass() {
        creds =
                new DatabaseCredentials(
                        "localhost",
                        3306,
                        "",
                        ""
                );

        dataSource = QueryDatabase.getDataSource(creds);
    }

    @Before
    public void setUp() throws SQLException {
        executeQuery("CREATE DATABASE IF NOT EXISTS mysql_agent_binlog_integration");
        dropTables();
    }

    @After
    public void tearDown() throws SQLException {
        dropTables();
        executeQuery("DROP DATABASE IF EXISTS mysql_agent_binlog_integration");
    }

    private static void dropTables() {
        dropTable("child");
        dropTable("parent");
    }

    @Test
    public void binaryLogDoesNotRecordCascadingDeletes() throws Exception {
        BinlogClient client = new BinlogClient(creds);
        List<SourceEvent> sourceEvents = new ArrayList<>();

        BinlogPosition startPosition = client.currentPosition();

        executeQuery("CREATE TABLE IF NOT EXISTS mysql_agent_binlog_integration.parent (id int, primary key(id))");
        executeQuery("CREATE TABLE IF NOT EXISTS mysql_agent_binlog_integration.child (id int, parent_ref int, primary key(id), foreign key(parent_ref) references mysql_agent_binlog_integration.parent (id) on delete cascade)");
        executeQuery("INSERT INTO mysql_agent_binlog_integration.parent values (1)");
        executeQuery("INSERT INTO mysql_agent_binlog_integration.child values (1, 1)");
        executeQuery("DELETE FROM mysql_agent_binlog_integration.parent WHERE id = 1");

        try (EventReader reader = client.events(startPosition)) {
            SourceEvent sourceEvent;
            while ((sourceEvent = reader.readEvent()).event != SourceEventType.TIMEOUT) {
                sourceEvents.add(sourceEvent);
            }
        }

        assertThat(sourceEvents.size(), equalTo(3));
        assertThat(sourceEvents.get(0).event, equalTo(SourceEventType.INSERT));
        assertThat(sourceEvents.get(1).event, equalTo(SourceEventType.INSERT));
        assertThat(sourceEvents.get(2).event, equalTo(SourceEventType.DELETE));
    }

    @Test
    public void binaryLogDoesNotRecordCascadingUpdates() throws Exception {
        BinlogClient client = new BinlogClient(creds);
        List<SourceEvent> sourceEvents = new ArrayList<>();

        BinlogPosition startPosition = client.currentPosition();

        executeQuery("CREATE TABLE IF NOT EXISTS mysql_agent_binlog_integration.parent (id int, primary key(id))");
        executeQuery("CREATE TABLE IF NOT EXISTS mysql_agent_binlog_integration.child (id int, parent_ref int, primary key(id), foreign key(parent_ref) references mysql_agent_binlog_integration.parent (id) on update cascade)");
        executeQuery("INSERT INTO mysql_agent_binlog_integration.parent values (1)");
        executeQuery("INSERT INTO mysql_agent_binlog_integration.child values (1, 1)");
        executeQuery("UPDATE mysql_agent_binlog_integration.parent SET id = 2 WHERE id = 1");

        try (EventReader reader = client.events(startPosition)) {
            SourceEvent sourceEvent;
            while ((sourceEvent = reader.readEvent()).event != SourceEventType.TIMEOUT) {
                sourceEvents.add(sourceEvent);
            }
        }

        assertThat(sourceEvents.size(), equalTo(3));
        assertThat(sourceEvents.get(0).event, equalTo(SourceEventType.INSERT));
        assertThat(sourceEvents.get(1).event, equalTo(SourceEventType.INSERT));
        assertThat(sourceEvents.get(2).event, equalTo(SourceEventType.UPDATE));
    }

    private static void dropTable(String table) {
        try {
            executeQuery("DROP TABLE IF EXISTS " + database + "." + table);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static void executeQuery(String query) throws SQLException {
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(true);
            try (Statement statement = connection.createStatement()) {
                statement.execute(query);
            }
        }
    }
}

