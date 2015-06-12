package com.yeahmobi.datasystem.query.memcache;
/**
 * Created by yangxu on 5/17/14.
 */

import com.fasterxml.jackson.annotation.JsonProperty;

public class MemCacheConfig {

    @JsonProperty
    String[] hostPorts = new String[] {"10.1.5.60:11211"};

    public String[] getHostPorts() {
        return hostPorts;
    }
}
