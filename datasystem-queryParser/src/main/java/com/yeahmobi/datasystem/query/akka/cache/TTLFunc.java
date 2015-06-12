package com.yeahmobi.datasystem.query.akka.cache;

/**
 * Created by yangxu on 5/9/14.
 */
public interface TTLFunc {

    /**
     * calculating obj ttl with given params
     * @param objs: dimensions
     * @return ttl in millis
     */
    public long apply(Object... objs);
}
