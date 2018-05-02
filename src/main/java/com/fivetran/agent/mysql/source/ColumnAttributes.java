package com.fivetran.agent.mysql.source;

import java.util.Optional;

public class ColumnAttributes {
    public final int ordinalPosition;
    public final String columnType;
    public final Optional<String> characterSetName;
    public final boolean primaryKey;
    public final Optional<ColumnRef> referencedColumn;

    public ColumnAttributes(int ordinalPosition, String columnType, Optional<String> characterSetName, boolean primaryKey, Optional<ColumnRef> referencedColumn) {
        this.ordinalPosition = ordinalPosition;
        this.columnType = columnType;
        this.characterSetName = characterSetName;
        this.primaryKey = primaryKey;
        this.referencedColumn = referencedColumn;
    }
}
