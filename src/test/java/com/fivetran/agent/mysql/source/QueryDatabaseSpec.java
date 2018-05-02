package com.fivetran.agent.mysql.source;

import com.fivetran.agent.mysql.Rows;
import com.fivetran.agent.mysql.config.DatabaseCredentials;
import com.google.common.collect.ImmutableMap;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;
import java.util.List;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

// This is an integration test that expects a local MySQL database to be present.
// Due to this dependency it is Ignored in automated tests. Run this manually when needed.
@Ignore
public class QueryDatabaseSpec {

    private PrepareDatabase prepare;
    private QueryDatabase query;

    {
        DatabaseCredentials credentials = new DatabaseCredentials("localhost", 3306, "root", "");

        prepare = new PrepareDatabase(credentials);
        prepare.sql("CREATE SCHEMA IF NOT EXISTS test_schema");
        prepare.sql("DROP TABLE IF EXISTS test_schema.foo");
        prepare.sql("CREATE TABLE test_schema.foo (id INT PRIMARY KEY, data TEXT)");
        prepare.sql("INSERT INTO test_schema.foo VALUES (1, 'a'), (2, 'b'), (3, 'c')");

        query = new QueryDatabase(credentials);
    }

    private static class PrepareDatabase extends QueryDatabase {

        public PrepareDatabase(DatabaseCredentials credentials) {
            super(credentials);
        }

        public void sql(String s) {
            try (Connection connection = super.connect();
                 Statement statement = connection.createStatement()) {
                statement.execute(s);
            } catch (SQLException e) {
                throw new DatabaseException(e);
            }
        }
    }

    @Test
    public void unlimitedRows() {

        try (Rows rows = query.unlimitedRows("SELECT id, data FROM test_schema.foo")) {
            Iterator<Row> r = rows.iterator();
            assertThat(r.next(), contains("1", "a"));
            assertThat(r.next(), contains("2", "b"));
            assertThat(r.next(), contains("3", "c"));
        }
    }

    @Test
    public void records() {

        List<Record> records = query.records("SELECT id, data FROM test_schema.foo");
        assertThat(records.get(0), equalTo(ImmutableMap.of("id", "1", "data", "a")));
        assertThat(records.get(1), equalTo(ImmutableMap.of("id", "2", "data", "b")));
        assertThat(records.get(2), equalTo(ImmutableMap.of("id", "3", "data", "c")));
    }

    @Test
    public void record() {

        Record record = query.record("SELECT 1 AS value");
        assertThat(record, equalTo(ImmutableMap.of("value", "1")));
    }

    // TODO: test all data types, make sure to test TIME type (previous mysql made special provisions for it)
}
