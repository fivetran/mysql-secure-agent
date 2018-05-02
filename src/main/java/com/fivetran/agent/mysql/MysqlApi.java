/**
* Copyright (c) Fivetran 2018
**/
package com.fivetran.agent.mysql;

import com.fivetran.agent.mysql.output.TableDefinition;
import com.fivetran.agent.mysql.source.ServerConfig;
import com.fivetran.agent.mysql.source.TableRef;

import java.util.Map;
import java.util.function.Supplier;

public class MysqlApi {

    final ImportTable importTable;
    final Runnable cleanup;
    final Supplier<ServerConfig> serverConfig;
    final Supplier<Map<TableRef, TableDefinition>> tableDefinitions;
    final ReadSourceLog readSourceLog;

    public MysqlApi(ImportTable importTable, Runnable cleanup, Supplier<ServerConfig> serverConfig, Supplier<Map<TableRef, TableDefinition>> tableDefinitions, ReadSourceLog readSourceLog) {
        this.importTable = importTable;
        this.cleanup = cleanup;
        this.serverConfig = serverConfig;
        this.tableDefinitions = tableDefinitions;
        this.readSourceLog = readSourceLog;
    }
}
