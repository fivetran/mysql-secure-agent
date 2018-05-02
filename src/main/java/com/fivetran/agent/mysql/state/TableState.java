/**
* Copyright (c) Fivetran 2018
**/
package com.fivetran.agent.mysql.state;

import java.util.Map;
import java.util.Optional;

public class TableState {
    public Optional<Map<String, String>> lastSyncedPrimaryKey = Optional.empty();
    public boolean finishedImport = false;
}
