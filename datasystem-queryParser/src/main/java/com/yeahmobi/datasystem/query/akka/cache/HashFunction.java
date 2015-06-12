package com.yeahmobi.datasystem.query.akka.cache;
/**
 * Created by yangxu on 5/9/14.
 */

public interface HashFunction {

    /**
     * get resulting key string with given input
     * @param input
     * @return
     */
    public String apply(String input);
}
