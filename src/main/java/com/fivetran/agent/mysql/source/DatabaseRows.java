package com.fivetran.agent.mysql.source;

import com.fivetran.agent.mysql.Main;
import com.fivetran.agent.mysql.Rows;
import com.fivetran.agent.mysql.log.LogGeneralException;

import java.sql.*;
import java.util.Iterator;
import java.util.List;

public class DatabaseRows implements Rows {

    private final Connection connection;
    private final Statement statement;
    private final ResultSet resultSet;
    private final List<String> columnNames;

    DatabaseRows(Connection connection, Statement statement, ResultSet resultSet, List<String> columnNames) {
        this.connection = connection;
        this.statement = statement;
        this.resultSet = resultSet;
        this.columnNames = columnNames;
    }

    @Override
    public List<String> columnNames() {
        return columnNames;
    }

    @Override
    public Iterator<Row> iterator() {
        return new Iterator<Row>() {

            private int numColumns;
            private boolean hasNext;

            {
                try {
                    ResultSetMetaData metaData = resultSet.getMetaData();
                    numColumns = metaData.getColumnCount();
                    hasNext = resultSet.next();
                } catch (SQLException e) {
                    throw new DatabaseException(e);
                }
            }

            @Override
            public boolean hasNext() {
                return hasNext;
            }

            @Override
            public Row next() {
                try {
                    Row row = new Row(numColumns);
                    for (int i = 1; i <= numColumns; i++)
                        row.add(resultSet.getString(i));

                    hasNext = resultSet.next();

                    return row;
                } catch (SQLException e) {
                    throw new DatabaseException(e);
                }
            }
        };
    }

    @Override
    public void close() {
        // Try closing the jdbc result set, statement and connection,
        // but ignore any errors because it means they were already closed
        try {
            resultSet.close();
        } catch (SQLException ignored) {
            Main.LOG.log(new LogGeneralException(ignored));
        } finally {
            try {
                statement.close();
            } catch (SQLException ignored) {
                Main.LOG.log(new LogGeneralException(ignored));
            } finally {
                try {
                    connection.close();
                } catch (SQLException ignored) {
                    Main.LOG.log(new LogGeneralException(ignored));
                }
            }
        }
    }
}
