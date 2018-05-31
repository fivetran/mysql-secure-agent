/**
* Copyright (c) Fivetran 2018
**/
package com.fivetran.agent.mysql.source;

import com.fivetran.agent.mysql.ImportTable;
import com.fivetran.agent.mysql.Rows;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.junit.Test;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class ImportSourceTableTest {

    @Test
    public void firstPage() {

        MockQuery mock = new MockQuery();
        ImportTable importTable = new ImportSourceTable(mock);

        TableRef table = new TableRef("test_schema", "foo");

        ImportTable.PagingParams params = new ImportTable.PagingParams(ImmutableList.of("id"), ImmutableList.of(), 1000);
        Rows rows = importTable.rows(table, ImmutableList.of("id", "data"), Optional.of(params));

        assertThat(mock.query, equalTo("SELECT `id`, `data`" +
                " FROM `test_schema`.`foo`" +
                " ORDER BY `id`" +
                " LIMIT 1000"));

        assertTrue(rows == mock.dummyRows);
    }

    @Test
    public void nextPage() {

        MockQuery mock = new MockQuery();
        ImportTable importTable = new ImportSourceTable(mock);

        TableRef table = new TableRef("test_schema", "foo");

        ImportTable.PagingParams params = new ImportTable.PagingParams(ImmutableList.of("id"), ImmutableList.of("1234"), 1000);
        Rows rows = importTable.rows(table, ImmutableList.of("id", "data"), Optional.of(params));

        assertThat(mock.query, equalTo("SELECT `id`, `data`" +
                " FROM `test_schema`.`foo`" +
                " WHERE (`id` > '1234')" +
                " ORDER BY `id`" +
                " LIMIT 1000"));

        assertTrue(rows == mock.dummyRows);
    }

    @Test
    public void noPaging() {

        MockQuery mock = new MockQuery();
        ImportTable importTable = new ImportSourceTable(mock);

        TableRef table = new TableRef("test_schema", "foo");
        Rows rows = importTable.rows(table, ImmutableList.of("data"), Optional.empty());

        assertThat(mock.query, equalTo("SELECT `data` FROM `test_schema`.`foo`"));

        assertTrue(rows == mock.dummyRows);
    }

    private static class MockQuery implements Query {

        String query = "";

        @Override
        public Rows unlimitedRows(String query) {
            this.query = query;
            return dummyRows;
        }

        @Override
        public List<Record> records(String query) {
            throw new RuntimeException("ImportSourceTable should never use allRows method because it will blow up memory usage");
        }

        @Override
        public Record record(String query) {
            throw new RuntimeException("ImportSourceTable has no business using singleRow method");
        }

        @Override
        public BinlogPosition target() {
            return null;
        }

        final Rows dummyRows = new Rows() {
            @Override
            public List<String> columnNames() {
                return null;
            }

            @Override
            public void close() {
            }

            @Override
            public Iterator<Row> iterator() {
                return null;
            }
        };
    }

    @Test
    public void multiColumnPagingClause() {

        ImportTable.PagingParams params = new ImportTable.PagingParams(ImmutableList.of("a", "b"), ImmutableList.of("1", "2"), 1);
        String pagingClause = ImportSourceTable.pagingClause(params);
        assertThat(pagingClause, equalTo(" WHERE (`a` > '1') OR (`a` = '1' AND `b` > '2') ORDER BY `a`, `b` LIMIT 1"));
    }

    @Test
    public void greaterThan() {

        assertThat(
                ImportSourceTable.greaterThan(list("a"), list("1")),
                equalTo("(`a` > '1')"));

        assertThat(
                ImportSourceTable.greaterThan(list("a", "b"), list("1", "2")),
                equalTo("(`a` > '1') OR (`a` = '1' AND `b` > '2')"));

        assertThat(
                ImportSourceTable.greaterThan(list("a", "b", "c"), list("1", "2", "3")),
                equalTo("(`a` > '1') OR (`a` = '1' AND `b` > '2') OR (`a` = '1' AND `b` = '2' AND `c` > '3')"));
    }

    private static List<String> list(String... strings) {
        return Lists.newArrayList(strings);
    }
}
