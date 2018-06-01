/**
* Copyright (c) Fivetran 2018
**/
package com.fivetran.agent.mysql.deserialize;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fivetran.agent.mysql.config.Config;
import com.fivetran.agent.mysql.credentials.Credentials;
import com.fivetran.agent.mysql.state.AgentState;

import java.io.IOException;
import java.io.InputStream;

public class Deserialize {
    private static ObjectMapper JSON = new ObjectMapper()
            .registerModules(new Jdk8Module())
            .configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);

    public static AgentState state() {
        return deserialize(Deserialize.class.getResourceAsStream("/state.json"), AgentState.class);
    }

    public static Config config() {
        return deserialize(Deserialize.class.getResourceAsStream("/config.json"), Config.class);
    }

    public static Credentials credentials() {
        return deserialize(Deserialize.class.getResourceAsStream("/credentials.json"), Credentials.class);
    }

    static <T> T deserialize(InputStream in, Class<T> type) {
        try {
            if (in.available() == 0)
                return type.newInstance();
            return JSON.readValue(in, type);
        } catch (InstantiationException | IllegalAccessException | IOException e) {
            throw new RuntimeException(e);
        }
    }
}
