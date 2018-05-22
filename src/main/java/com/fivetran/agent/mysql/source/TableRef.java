/**
* Copyright (c) Fivetran 2018
**/
package com.fivetran.agent.mysql.source;

//@JsonDeserialize(using = TableRefDeserializer.class, as = )
public class TableRef {
    public final String schema, name;

    public TableRef(String schemaName, String tableName) {
        this.schema = schemaName;
        this.name = tableName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TableRef tableRef = (TableRef) o;

        return schema.equals(tableRef.schema) && name.equals(tableRef.name);
    }

    @Override
    public int hashCode() {
        int result = schema.hashCode();
        result = 31 * result + name.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return escapePeriods(schema) + '.' + escapePeriods(name);
    }

    String escapePeriods(String name) {
        return name.replaceAll("\\\\", "\\\\\\\\").replaceAll("\\.", "\\\\.");
    }
}
