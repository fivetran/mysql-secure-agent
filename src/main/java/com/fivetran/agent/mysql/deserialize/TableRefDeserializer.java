package com.fivetran.agent.mysql.deserialize;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fivetran.agent.mysql.source.TableRef;

import java.io.IOException;

public class TableRefDeserializer extends JsonDeserializer {
    @Override
    public Object deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
        String[] split = p.getValueAsString().split("\\.");
        return new TableRef(split[0], split[1]);
    }
}
