package com.yeahmobi.datasystem.query.akka.cache;
/**
 * Created by yangxu on 5/6/14.
 */

import com.google.common.base.Strings;
import org.apache.log4j.Logger;

public class QueryCacheFactory {

    private static Logger logger = Logger.getLogger(QueryCacheFactory.class);

    public static QueryCache create(String type) {
        if (logger.isDebugEnabled()) {
            logger.debug("Application CacheType : [" + type + "].");
        }
        if (!Strings.isNullOrEmpty(type)) {
            if ("redis".equals(type.toLowerCase())) {
                return RedisQueryCache.getInstance();
            } else if ("memcache".equals(type.toLowerCase())) {
                return MemCacheQueryCache.getInstance();
            }
        }
        return null;

    }
}
