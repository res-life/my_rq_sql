package com.yeahmobi.datasystem.query.akka.cache;

import org.apache.log4j.Logger;

import com.yeahmobi.datasystem.query.antlr4.DruidReportParser;
import com.yeahmobi.datasystem.query.reportrequest.ReportParam;

/**
 * Created by yangxu on 5/9/14.
 */

public class CacheToolChest implements CacheTool {

    private final static Logger logger = Logger.getLogger(CacheToolChest.class);

    public QueryCache cache;
    public TTLFunc ttlFunc;

    public CacheToolChest(QueryCache cache, TTLFunc ttlFunc) {
        this.cache = cache;
        this.ttlFunc = ttlFunc;
    }

    public Object get(Object obj, Class<?> clazz) {
        if (null == cache || ttlFunc == null) return null;
        ReportParam reportRequest = (ReportParam)obj;
        
        // L1 cache 保存当前页
        Object key = cache.cacheKey(reportRequest.toStringForL1());
        Object val = cache.get(key, clazz);
        if (logger.isDebugEnabled()) {
            logger.debug("Cache-GET:" + key + "," + val);
        }
        return val;
    }

    @Override
    public boolean set(Object... args) {
        if (null == cache || ttlFunc == null) return false;
        if (null == args || args.length != 2)
            return false;

        ReportParam reportRequest = (ReportParam) args[0];

        Object key = cache.cacheKey(reportRequest.toStringForL1());
        long ttl = ttlFunc.apply(reportRequest);
        if (logger.isDebugEnabled()) {
            logger.debug("Cache-ADD:" + key + "," + ttl);
        }
        cache.set(key, args[1], ttl);
        return true;
    }
}
