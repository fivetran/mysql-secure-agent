/**
 * Copyright (c) Fivetran 2018
 **/
package com.fivetran.agent.mysql;

import com.fivetran.agent.mysql.config.Config;
import com.fivetran.agent.mysql.output.Event;
import com.fivetran.agent.mysql.output.TableDefinition;
import com.fivetran.agent.mysql.source.BinlogPosition;
import com.fivetran.agent.mysql.source.TableRef;
import com.fivetran.agent.mysql.state.AgentState;

public class BatchUpdater extends Updater {

    public BatchUpdater(Config config, MysqlApi mysql, Output out, Log log, AgentState state) {
        super(config, mysql, out, log, state);
    }

    @Override
    public void update() throws Exception {
        updateState();

        do {
            sync();
        } while (!finishedImport());
    }

    @Override
    void sync() throws Exception {
        TableRef tableToImport = findTableToImport();

        if (tableToImport != null)
            syncPageFromTable(tableToImport);

        syncFromBinlog(mysql.readSourceLog.currentPosition());
    }

    private boolean finishedImport() {
        return state.tableStates.values().stream().allMatch(tableState -> tableState.finishedImport);
    }
}
