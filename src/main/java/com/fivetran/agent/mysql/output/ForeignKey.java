package com.fivetran.agent.mysql.output;

import java.util.ArrayList;
import java.util.List;

/**
 * A foreign key in MySQL can contain many columns. A composite foreign key must point to a set
 * of columns in another table with the same cardinality.
 */
public class ForeignKey {
    public List<String> columns = new ArrayList<>();
    public List<String> referencedColumns = new ArrayList<>();

    public ForeignKey() {}

    public ForeignKey(String column, String referencedColumn) {
        this.columns.add(column);
        this.referencedColumns.add(referencedColumn);
    }

    public ForeignKey(List<String> columns, List<String> referencedColumns) {
        this.columns = columns;
        this.referencedColumns = referencedColumns;
    }
}
