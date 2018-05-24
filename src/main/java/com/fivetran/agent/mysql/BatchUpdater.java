/**
 * Copyright (c) Fivetran 2018
 **/
package com.fivetran.agent.mysql;

import com.fivetran.agent.mysql.config.Config;
import com.fivetran.agent.mysql.source.BinlogPosition;
import com.fivetran.agent.mysql.source.TableRef;
import com.fivetran.agent.mysql.state.AgentState;

public class BatchUpdater extends Updater {

    public BatchUpdater(Config config, MysqlApi mysql, Output out, Log log, AgentState state) {
        super(config, mysql, out, log, state);
    }

    @Override
    public void update() throws Exception {
        updateTableDefinitions();

        do {
            sync();
        } while (!finishedImport());
    }

    @Override
    void sync() throws Exception {
        TableRef tableToImport = findTableToImport();

        // Record binlog position prior to beginning sync, to guarantee we capture every event that's occurred
        // since the beginning of the sync
        BinlogPosition target = mysql.readSourceLog.currentPosition();

        if (tableToImport != null)
            syncPageFromTable(tablesToSync.get(tableToImport));

        syncFromBinlog(target);
    }

    private boolean finishedImport() {
        return state.tables.values().stream().allMatch(tableState -> tableState.finishedImport);
    }
}
