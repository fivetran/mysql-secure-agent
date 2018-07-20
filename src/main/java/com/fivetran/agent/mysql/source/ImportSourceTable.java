/**
* Copyright (c) Fivetran 2018
**/
package com.fivetran.agent.mysql.source;

import com.fivetran.agent.mysql.ImportTable;
import com.fivetran.agent.mysql.Rows;
import com.fivetran.agent.mysql.output.ColumnDefinition;
import com.google.common.base.Joiner;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static java.util.stream.Collectors.joining;

public class ImportSourceTable implements ImportTable {

    private final Query query;

    public ImportSourceTable(Query query) {
        this.query = query;
    }

    @Override
    public Rows rows(TableRef table, List<ColumnDefinition> selectColumns, Optional<PagingParams> pagingParams) {

        String select = "SELECT " + columnToSelect(selectColumns)
                + " FROM " + quoted(table)
                + pagingParams.map(ImportSourceTable::pagingClause).orElse("");

        return query.unlimitedRows(select);
    }

    static String pagingClause(PagingParams p) {
        String whereClause = p.startAfterValue.isEmpty()
                ? ""
                : " WHERE " + greaterThan(p.orderByColumns, p.startAfterValue);
        // orderByColumns is empty during the very first select query on a table
        String orderByClause = p.orderByColumns.isEmpty()
                ? ""
                : " ORDER BY " + quoted(p.orderByColumns);
        String limitClause = " LIMIT " + p.limitRows;
        return whereClause + orderByClause + limitClause;
    }

    static String greaterThan(List<String> orderByColumns, List<String> startAfterValue) {

        if (orderByColumns.size() != startAfterValue.size())
            throw new RuntimeException("Number of order-by columns and values must match");

        if (orderByColumns.size() == 0)
            throw new RuntimeException("There must be at least one order-by column");

        StringBuilder whereClause = new StringBuilder();
        for (int i = 0; i < orderByColumns.size(); i++) {
            if (i > 0) whereClause.append(" OR ");
            whereClause.append("(");
            for (int j = 0; j <= i; j++) {

                if (j > 0) whereClause.append(" AND ");

                String column = orderByColumns.get(j);
                String value = startAfterValue.get(j);
                assert value != null;

                String comparison = quoted(column) + (j == i ? " > '" : " = '") + value + "'";
                whereClause.append(comparison);
            }
            whereClause.append(")");
        }
        return whereClause.toString();
    }

    private static String quoted(String identifier) {
        return '`' + identifier.replace("`", "``") + '`';
    }

    private static String quoted(TableRef table) {
        return quoted(table.schema) + '.' + quoted(table.name);
    }

    private static String quoted(List<String> columns) {
        return columns.stream().map(ImportSourceTable::quoted).collect(joining(", "));
    }

    private String columnToSelect(List<ColumnDefinition> columns) {
        List<String> colSpecs = new ArrayList<>();
        for (ColumnDefinition c : columns) {
            String colSpec;
            if (c.type.matches("time(?:\\(\\d+\\))?"))
                colSpec = "CAST(" + quoted(c.name) + " AS CHAR) AS " + quoted(c.name);
            else
                colSpec = quoted(c.name);

            colSpecs.add(colSpec);
        }
        return Joiner.on(", ").join(colSpecs);
    }

}
