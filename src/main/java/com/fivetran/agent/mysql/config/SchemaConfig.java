package com.fivetran.agent.mysql.config;

import java.util.HashMap;
import java.util.Map;

public class SchemaConfig {
    public boolean selected = true;
    public final Map<String, TableConfig> tables = new HashMap<>();

    @Override
    public String toString() {
        return "SchemaConfig{" +
                "selected=" + selected +
                ", tables=" + tables +
                ", selectOtherTables=" + selectOtherTables +
                '}';
    }

    public boolean selectOtherTables = true;
}
