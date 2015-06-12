package com.yeahmobi.datasystem.query.redis;
/**
 * Created by yangxu on 5/7/14.
 */

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Joiner;

import javax.validation.constraints.Min;

public class HAJedisConfig {

    @JsonProperty
    String[] hostPorts = new String[] {"localhost:6379"};

    @JsonProperty
    String[] ranks = new String[] {"2"};

    @JsonProperty
    String password = "";

    @JsonProperty @Min(10)
    int checkInterval = 100;

    @JsonIgnore
    String tag = "";

    public String[] getHostPorts() {
        return hostPorts;
    }

    public String[] getRanks() {
        return ranks;
    }

    public String getPassword() {
        return password;
    }

    public int getCheckInterval() {
        return checkInterval;
    }

    public String getTag() {
        return tag;
    }

    public void setTag(String tag) {
        this.tag = tag;
    }

    public String toString() {
        return ("HAJedisConfig[" + tag + "," + Joiner.on(' ').join(hostPorts)
                + "," + Joiner.on(' ').join(ranks) + ","
                + password + "," + checkInterval + "]");
    }
}
