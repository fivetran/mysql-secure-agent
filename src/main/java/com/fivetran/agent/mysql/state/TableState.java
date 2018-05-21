/**
* Copyright (c) Fivetran 2018
**/
package com.fivetran.agent.mysql.state;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public class TableState {
    public Optional<Map<String, String>> lastSyncedPrimaryKey = Optional.empty();
    public boolean finishedImport = false;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TableState that = (TableState) o;
        return finishedImport == that.finishedImport &&
                Objects.equals(lastSyncedPrimaryKey, that.lastSyncedPrimaryKey);
    }

    @Override
    public int hashCode() {

        return Objects.hash(lastSyncedPrimaryKey, finishedImport);
    }
}
