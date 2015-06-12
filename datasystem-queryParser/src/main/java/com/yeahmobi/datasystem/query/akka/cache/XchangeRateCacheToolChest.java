package com.yeahmobi.datasystem.query.akka.cache;

import org.apache.log4j.Logger;

import com.google.gson.Gson;
import com.yeahmobi.datasystem.query.meta.XCHANGE_RATE_BASE;

public class XchangeRateCacheToolChest implements CacheTool {
    private Logger logger = Logger.getLogger(XchangeRateCacheToolChest.class);

    public XchangeRateCacheToolChest(QueryCache cache, TTLFunc ttlFunc) {
        this.cache = cache;
        this.ttlFunc = ttlFunc;
    }

    @Override public Object get(Object obj, Class<?> clazz) {
        XCHANGE_RATE_BASE yahoo_xchange_rate_base = (XCHANGE_RATE_BASE) obj;
        String key = "xr-" + yahoo_xchange_rate_base.getCurrency_from() + yahoo_xchange_rate_base.getCurrency_to();
        Object val = cache.get(key, clazz);
        if (logger.isDebugEnabled()) {
            logger.debug("XchangeRate-Cache-GET:" + "key" + "," + new Gson().toJson(val));
        }
        return val;
    }

    /*cacheTool.set(from,to,value)*/
    @Override public boolean set(Object... args) {    
        boolean result = false;
        if (null != cache && null != ttlFunc && null != args && args.length == 3) {
            String key = "xr-" + String.valueOf(args[0]) + String.valueOf(args[1]);
            int ttl = (int) ttlFunc.apply(key);
            if (logger.isDebugEnabled()) {
                logger.debug("XchangeRate-Cache-ADD:" + key + "," + ttl);
            }
            // cache.set(key, String.valueOf(args[2]), ttl);
            cache.set(key, args[2], ttl);
            result = true;
        }
        
        return result;
    }
    
    private QueryCache cache;
    private TTLFunc ttlFunc;
}
