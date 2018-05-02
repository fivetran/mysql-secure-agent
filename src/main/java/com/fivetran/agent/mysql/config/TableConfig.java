/**
* Copyright (c) Fivetran 2018
**/
package com.fivetran.agent.mysql.config;

import java.util.HashMap;
import java.util.Map;

public class TableConfig {
    public boolean selected = true;
    public final Map<String, ColumnConfig> columns = new HashMap<>();
    public boolean selectOtherColumns = true;

    // Empty constructor for Jackson
    public TableConfig() {}

    public TableConfig(boolean selected, boolean selectOtherColumns) {
        this.selected = selected;
        this.selectOtherColumns = selectOtherColumns;
    }

    @Override
    public String toString() {
        return "TableConfig{" +
                "selected=" + selected +
                ", columns=" + columns +
                ", selectOtherColumns=" + selectOtherColumns +
                '}';
    }
}
