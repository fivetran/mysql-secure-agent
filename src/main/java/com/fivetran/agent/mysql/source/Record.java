package com.fivetran.agent.mysql.source;

import java.util.HashMap;
import java.util.Map;

public class Record extends HashMap<String, String> {

    public Record(int initialCapacity) {
        super(initialCapacity);
    }

    public Record(Map<String, String> values) {
        super(values);
    }
}
