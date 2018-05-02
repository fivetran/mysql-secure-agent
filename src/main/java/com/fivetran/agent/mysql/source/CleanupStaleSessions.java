package com.fivetran.agent.mysql.source;

public class CleanupStaleSessions implements Runnable {

    /**
     * Mysql tends to leave orphaned sessions lying around on the server side
     * upon unclean disconnect. However rarely this happens, they accumulate
     * over time and therefore must be cleaned up, or else we will run
     * into the connection limit which would prevent any further connections
     * to the server.
     */
    @Override
    public void run() {
        throw new UnsupportedOperationException("Not yet implemented");
    }
}
