/**
 * Copyright (c) Fivetran 2018
 **/
package com.fivetran.agent.mysql;

import com.fivetran.agent.mysql.config.Config;
import com.fivetran.agent.mysql.output.BucketOutput;
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

        // TODO talk to meel about what to do here
        if (out instanceof BucketOutput)
            ((BucketOutput) out).flushToBucket(state);
    }

    @Override
    void sync() throws Exception {
        TableRef tableToImport = findTableToImport();

        if (tableToImport != null)
            syncPageFromTable(tablesToSync.get(tableToImport));

        syncFromBinlog(mysql.readSourceLog.currentPosition());
    }

    private boolean finishedImport() {
        return state.tables.values().stream().allMatch(tableState -> tableState.finishedImport);
    }
}
