package com.yeahmobi.datasystem.query.akka.cache;

/**
 * Created by yangxu on 5/9/14.
 */
public interface CacheTool {

    public Object get(Object ctx, Class<?> clazz);

    public boolean set(Object... args);
}
