package com.fivetran.agent.mysql;

import com.fivetran.agent.mysql.source.Row;

import java.util.List;

public interface Rows extends Iterable<Row>, AutoCloseable {

    List<String> columnNames();

    // Override AutoCloseable.close to drop the checked exception
    void close();
}
