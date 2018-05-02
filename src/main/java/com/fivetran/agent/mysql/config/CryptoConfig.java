/**
* Copyright (c) Fivetran 2018
**/
package com.fivetran.agent.mysql.config;

public class CryptoConfig {
    /**
     * This salt is appended to every value before it is hashed to protect against dictionary attacks
     */
    public String salt = "";
}
