package com.yeahmobi.datasystem.config;
/**
 * Created by yangxu on 5/7/14.
 */

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.log4j.Logger;

import javax.validation.constraints.NotNull;

public class Config {

    private static Logger logger = Logger.getLogger(Config.class);

    @JsonProperty
    String f1 = "f1";

    @JsonProperty
    @NotNull
    String f2 = "f2";

    public String getF1() {
        return f1;
    }

    public String getF2() {
        return f2;
    }
}
