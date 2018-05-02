package com.fivetran.agent.mysql.source;

import com.fivetran.agent.mysql.Rows;
import com.fivetran.agent.mysql.config.DatabaseCredentials;
import com.mysql.jdbc.jdbc2.optional.MysqlDataSource;

import javax.sql.DataSource;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class QueryDatabase implements Query {

    private final DatabaseCredentials credentials;
    int timeoutSeconds = 60;

    public QueryDatabase(DatabaseCredentials credentials) {
        this.credentials = credentials;
    }

    public static DataSource getDataSource(DatabaseCredentials credentials) {

        // Connect to MySQL database
        MysqlDataSource source = new MysqlDataSource();
        source.setServerName(credentials.host);
        source.setPortNumber(credentials.port);
        source.setUser(credentials.user);
        source.setPassword(credentials.password);
        source.setZeroDateTimeBehavior("convertToNull");
        source.setCharacterEncoding("UTF-8");

        return source;
    }

    protected Connection connect() {

        try {
            DataSource source = getDataSource(credentials);
            DriverManager.setLoginTimeout(timeoutSeconds);
            return source.getConnection();

        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    @Override
    public Rows unlimitedRows(String query) {

        try {
            Connection connection = connect();
            Statement statement = connection.createStatement();
            limitNumberOfResultSetRowsInMemory(statement);
            ResultSet resultSet = statement.executeQuery(query);

            ResultSetMetaData metaData = resultSet.getMetaData();
            int numColumns = metaData.getColumnCount();
            List<String> columnNames = new ArrayList<>(numColumns);
            for (int i = 1; i <= numColumns; i++) {
                columnNames.add(metaData.getColumnLabel(i));
            }

            return new DatabaseRows(connection, statement, resultSet, columnNames);

        } catch (SQLException e) {
            throw new DatabaseException("Error while trying to run the query: " + query, e);
        }
    }

    private void limitNumberOfResultSetRowsInMemory(Statement statement) throws SQLException {
        statement.setFetchSize(Integer.MIN_VALUE);
    }

    @Override
    public List<Record> records(String query) {

        try (Rows results = unlimitedRows(query)) {
            List<String> columnNames = results.columnNames();
            List<Record> records = new ArrayList<>();
            results.forEach(row -> {
                Record record = new Record(columnNames.size());
                for (int i = 0; i < columnNames.size(); i++) {
                    record.put(columnNames.get(i), row.get(i));
                }
                records.add(record);
            });
            return records;
        }
    }

    @Override
    public Record record(String query) {

        List<Record> records = records(query);
        if (records.size() == 0) throw new RuntimeException("Query returned no results");
        if (records.size() != 1) throw new RuntimeException("Query returned more than one row");
        return records.get(0);
    }
}
