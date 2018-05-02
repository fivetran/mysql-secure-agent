package com.fivetran.agent.mysql.source;

public class ServerConfig {
    public final DbType dbType;
    public final String mysqlVersion;
    public final boolean replicationClientPrivilege;
    public final boolean replicationSlavePrivilege;
    public final boolean binlogEnabled;
    public final LogFormat logFormat;
    public final RowImage rowImage;
    public final String serverId;
    public final boolean logSlaveUpdates;

    public ServerConfig(DbType dbType,
                        String mysqlVersion,
                        boolean replicationClientPrivilege,
                        boolean replicationSlavePrivilege,
                        boolean binlogEnabled,
                        LogFormat logFormat,
                        RowImage rowImage,
                        String serverId,
                        boolean logSlaveUpdates) throws MysqlConfigException, MysqlUserException {
        this.dbType = dbType;
        this.mysqlVersion = mysqlVersion;
        this.replicationClientPrivilege = replicationClientPrivilege;
        this.replicationSlavePrivilege = replicationSlavePrivilege;
        this.binlogEnabled = binlogEnabled;
        this.logFormat = logFormat;
        this.rowImage = rowImage;
        this.serverId = serverId;
        this.logSlaveUpdates = logSlaveUpdates;
        checkConfig();
    }

    public enum DbType {
        AURORA,
        MYSQL,
        RDS
    }

    public enum RowImage {
        FULL,
        MINIMAL,
        NOBLOB
    }

    public enum LogFormat {
        STATEMENT,
        MIXED,
        ROW,
        OFF
    }

    enum ConfigError {
        EnableBinlog("Binary logging must be enabled: SHOW MASTER STATUS must return a non-empty result"),
        BinlogFormat("The 'binlog_format' server configuration variable must be set to ROW"),
        BinlogRowImage("The 'binlog_row_image' server configuration variable must be set to FULL"),
        LogSlaveUpdates("The 'log_slave_updates' server configuration variable must be set to 1"),
        SetServerId("The 'server-id' must be set for Binlog processing");

        public final String longMessage;

        ConfigError(String longMessage) {
            this.longMessage = longMessage;
        }
    }

    enum UserError {
        ReplicationClient("REPLICATION CLIENT privilege needed"),
        ReplicationSlave("REPLICATION SLAVE privilege needed");

        public final String longMessage;

        UserError(String longMessage) {
            this.longMessage = longMessage;
        }
    }

    private void checkConfig() throws MysqlUserException, MysqlConfigException {
        checkUser();
        if (serverId == null) throw new MysqlConfigException(ConfigError.SetServerId);
        if (!binlogEnabled) throw new MysqlConfigException(ConfigError.EnableBinlog);
        if (logFormat != LogFormat.ROW) throw new MysqlConfigException(ConfigError.BinlogFormat);
        if (rowImage != RowImage.FULL) throw new MysqlConfigException(ConfigError.BinlogRowImage);
        // todo: should there be an Optional<SlaveStatus> like in MysqlStatus#checkConfig?
        if (!logSlaveUpdates) throw new MysqlConfigException(ConfigError.LogSlaveUpdates);
    }

    private void checkUser() throws MysqlUserException {

        if (!replicationClientPrivilege)
            throw new MysqlUserException(UserError.ReplicationClient);
        if (!replicationSlavePrivilege) throw new MysqlUserException(UserError.ReplicationSlave);
    }
}
