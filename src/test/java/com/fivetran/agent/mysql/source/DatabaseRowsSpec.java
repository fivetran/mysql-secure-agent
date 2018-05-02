/**
* Copyright (c) Fivetran 2018
**/
package com.fivetran.agent.mysql.source;

import com.fivetran.agent.mysql.Rows;
import com.fivetran.agent.mysql.source.dummy.DummyConnection;
import com.fivetran.agent.mysql.source.dummy.DummyResultSet;
import com.fivetran.agent.mysql.source.dummy.DummyResultSetMetaData;
import com.fivetran.agent.mysql.source.dummy.DummyStatement;
import com.google.common.collect.ImmutableList;
import org.junit.Test;

import java.sql.*;
import java.util.Iterator;
import java.util.List;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.*;

public class DatabaseRowsSpec {

    @Test
    public void testQueryResults() throws SQLException {

        Connection connection = new DummyConnection() {
            boolean closed = false;

            @Override
            public void close() throws SQLException {
                closed = true;
            }
        };

        Statement statement = new DummyStatement() {
            boolean closed = false;

            @Override
            public void close() throws SQLException {
                closed = true;
            }
        };

        ResultSet resultSet = new DummyResultSet() {
            boolean closed = false;

            @Override
            public void close() throws SQLException {
                closed = true;
            }

            private String[][] data = {{"a", "b"}, {"i", "j"}, {"x", "y"}};
            private int i = -1;

            @Override
            public ResultSetMetaData getMetaData() throws SQLException {
                return new DummyResultSetMetaData() {
                    @Override
                    public int getColumnCount() throws SQLException {
                        return data[0].length;
                    }
                };
            }

            @Override
            public boolean first() throws SQLException {
                i = 0;
                return data.length > 0;
            }

            @Override
            public boolean next() throws SQLException {
                return ++i < data.length;
            }

            @Override
            public String getString(int columnIndex) throws SQLException {
                return data[i][columnIndex - 1];
            }
        };

        List<String> columnNames = ImmutableList.of("col1", "col2");
        try (Rows rows = new DatabaseRows(connection, statement, resultSet, columnNames)) {

            assertThat(rows.columnNames(), equalTo(columnNames));

            Iterator<Row> itr = rows.iterator();
            assertTrue(itr.hasNext());
            assertThat(itr.next(), contains("a", "b"));
            assertThat(itr.next(), contains("i", "j"));
            assertThat(itr.next(), contains("x", "y"));
            assertFalse(itr.hasNext());
        }
    }

}
