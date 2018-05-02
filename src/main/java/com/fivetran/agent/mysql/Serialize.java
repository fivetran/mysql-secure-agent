/**
* Copyright (c) Fivetran 2018
**/
package com.fivetran.agent.mysql;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fivetran.agent.mysql.state.AgentState;

public class Serialize {
    private static ObjectMapper JSON = new ObjectMapper()
            .registerModules(new Jdk8Module())
            .configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);

    public static String state(AgentState state) {
        try {
            return JSON.writerWithDefaultPrettyPrinter().writeValueAsString(state);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
