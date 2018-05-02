package com.fivetran.agent.mysql.source;

//@JsonDeserialize(using = TableRefDeserializer.class, as = )
public class TableRef {
    public final String schemaName, tableName;

    public TableRef(String schemaName, String tableName) {
        this.schemaName = schemaName;
        this.tableName = tableName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TableRef tableRef = (TableRef) o;

        return schemaName.equals(tableRef.schemaName) && tableName.equals(tableRef.tableName);
    }

    @Override
    public int hashCode() {
        int result = schemaName.hashCode();
        result = 31 * result + tableName.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return escapePeriods(schemaName) + '.' + escapePeriods(tableName);
    }

    String escapePeriods(String name) {
        return name.replaceAll("\\\\", "\\\\\\\\").replaceAll("\\.", "\\\\.");
    }
}
