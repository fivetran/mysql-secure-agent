/**
* Copyright (c) Fivetran 2018
**/
package com.fivetran.agent.mysql.source;

import com.fivetran.agent.mysql.Rows;

import java.util.List;

public interface Query {
    /**
     * Run a query and allow an unlimited number of rows to be fetched
     * by not keeping them all in memory. Make sure to close the
     * Rows object when finished.
     */
    Rows unlimitedRows(String query);

    /**
     * Run a query and return all rows
     */
    List<Record> records(String query);

    /**
     * Run a query and return only first row
     */
    Record record(String query);
}
