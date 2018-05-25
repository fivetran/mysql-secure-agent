/**
* Copyright (c) Fivetran 2018
**/
package com.fivetran.agent.mysql.deserialize;

import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.KeyDeserializer;
import com.fivetran.agent.mysql.source.TableRef;

import java.io.IOException;

public class TableRefDeserializer extends KeyDeserializer {
    @Override
    public TableRef deserializeKey(String key, DeserializationContext ctxt) throws IOException {
        String[] split = key.split("\\.");
        return new TableRef(split[0], split[1]);
    }
}
