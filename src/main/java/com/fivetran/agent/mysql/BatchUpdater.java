package com.fivetran.agent.mysql;

import com.fivetran.agent.mysql.config.Config;
import com.fivetran.agent.mysql.source.BinlogPosition;
import com.fivetran.agent.mysql.source.TableRef;
import com.fivetran.agent.mysql.state.AgentState;

import java.util.function.Supplier;

public class BatchUpdater extends Updater {
    private final Supplier<BinlogPosition> target;

    public BatchUpdater(Config config, MysqlApi mysql, Output out, Log log, AgentState state, Supplier<BinlogPosition> target) {
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
        syncFromBinlog(target.get());
    }

    private boolean done() {
        return state.tables.values().stream().allMatch(tableState -> tableState.finishedImport)
                && state.binlogPosition.equals(target);
    }
}
