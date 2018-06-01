package com.fivetran.agent.mysql.serialize;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

import java.io.IOException;

public class TableRefSerializer extends JsonSerializer {
    @Override
    public void serialize(Object value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
        gen.writeString(value.toString());
    }
}
