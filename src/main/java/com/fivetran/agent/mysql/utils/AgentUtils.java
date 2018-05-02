package com.fivetran.agent.mysql.utils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AgentUtils {

    public static  <K, V> Map<K, V> map(List<K> keys, List<V> values) {
        if (keys.size() != values.size())
            throw new RuntimeException("Keys (length " + keys.size() + ") and values (" + values.size() + ") did not have same size, couldn't create map");
        Map<K, V> map = new HashMap<>();
        for (int i = 0; i < keys.size(); i++) {
            map.put(keys.get(i), values.get(i));
        }
        return map;
    }
}
