package com.fivetran.agent.mysql;

import com.fivetran.agent.mysql.config.Config;
import com.fivetran.agent.mysql.source.BinlogPosition;
import com.fivetran.agent.mysql.source.TableRef;
import com.fivetran.agent.mysql.state.AgentState;

public class BatchUpdater extends Updater {
    private final BinlogPosition target;

    public BatchUpdater(Config config, MysqlApi mysql, Output out, Log log, AgentState state, BinlogPosition target) {
        super(config, mysql, out, log, state);
        this.target = target;
    }

    @Override
    public void update() throws Exception {
        updateTableDefinitions();

        while (!done()) {
            sync();
        }
    }

    @Override
    void sync() throws Exception {
        TableRef tableToImport = findTableToImport();
        syncPageFromTable(tablesToSync.get(tableToImport));
        syncFromBinlog(target);
    }

    private boolean done() {
        return state.tables.values().stream().allMatch(tableState -> tableState.finishedImport)
                && state.binlogPosition.equals(target);
    }
}
