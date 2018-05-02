/**
* Copyright (c) Fivetran 2018
**/
package com.fivetran.agent.mysql.source;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Row extends ArrayList<String> {

    public Row(int initialCapacity) {
        super(initialCapacity);
    }

    public Row(String... values) {
        super(Arrays.asList(values));
    }

    public int getColumnCount() {
        return this.size();
    }
}
