package com.fivetran.agent.mysql.output;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

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
        assert columns.size() == referencedColumns.size();
        this.columns = columns;
        this.referencedColumns = referencedColumns;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ForeignKey that = (ForeignKey) o;
        return Objects.equals(columns, that.columns) &&
                Objects.equals(referencedColumns, that.referencedColumns);
    }

    @Override
    public int hashCode() {

        return Objects.hash(columns, referencedColumns);
    }
}
