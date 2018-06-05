/**
* Copyright (c) Fivetran 2018
**/
package com.fivetran.agent.mysql.serialize;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;

public class Serialize {
    private static ObjectMapper JSON = new ObjectMapper()
            .registerModules(new Jdk8Module())
            .configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);

    public static String value(Object anything) {
        try {
            return JSON.writerWithDefaultPrettyPrinter().writeValueAsString(anything);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
