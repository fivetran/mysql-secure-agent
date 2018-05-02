package com.fivetran.agent.mysql.config;

import java.util.Optional;

public class ColumnConfig {
    public boolean selected = true;
    public boolean hash = false;
    public Optional<Boolean> implicitKey = Optional.empty();

    // Empty constructor for Jackson
    public ColumnConfig() {}

    public ColumnConfig(boolean selected, boolean hash, Optional<Boolean> implicitKey) {
        this.selected = selected;
        this.hash = hash;
        this.implicitKey = implicitKey;
    }
}
