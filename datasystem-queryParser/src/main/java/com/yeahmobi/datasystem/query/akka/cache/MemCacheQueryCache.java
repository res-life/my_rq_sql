package com.yeahmobi.datasystem.query.akka.cache;
/**
 * Created by yangxu on 5/17/14.
 */

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.gson.Gson;
import com.yeahmobi.datasystem.query.memcache.MemCacheFactory;

import net.spy.memcached.MemcachedClient;

import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.concurrent.TimeUnit;

public class MemCacheQueryCache<K, V> implements QueryCache<K, V> {

    private static Logger logger = Logger.getLogger(MemCacheQueryCache.class);

    private static MemCacheQueryCache instance = new MemCacheQueryCache();
    public static MemCacheQueryCache getInstance() {
        return instance;
    }

    MemcachedClient client = MemCacheFactory.getInstance().get("query");

    private final static ObjectMapper mapper = new ObjectMapper();

    HashFunction hasher = UniqHashFunction.Md5;

    private MemCacheQueryCache() {
        if (null == client) {
            logger.error("unable to allocate a memcached client");
        }
    }

    @Override
    public void set(K key, V object) {
        set(key, object, (int)TimeUnit.DAYS.toSeconds(30) - 1);
    }

    @Override
    public void set(K key, V object, long ttl) {

        if (null == client || null == key || null == object) {
            return;
        }
        try {
            String val = mapper.writeValueAsString(object);
            client.set((String) cacheKey(key), (int) ttl, val);
        } catch (JsonProcessingException e) {
            logger.error(key + "->" + object, e);
        }
    }

    @Override
    public String get(K key) {
        Object object = client.get((String)cacheKey(key));
        String res = null;
        if (null != object) {
            try {
                res = mapper.writeValueAsString(object);
            } catch (JsonProcessingException e) {
                logger.error("", e);
            }
        }
        return res;
    }

    @Override
    public V get(K key, Class<V> clazz) {
        try {
            Object hashKey = cacheKey(key);
            Object object = client.get(String.valueOf(hashKey));
            String val = String.valueOf(object);
            if (!Strings.isNullOrEmpty(val)) {
                return mapper.readValue(val.getBytes("UTF-8"), clazz);
            }
        } catch (JsonProcessingException e) {
            logger.error(key, e);
        } catch (UnsupportedEncodingException e) {
            logger.error(key, e);
        } catch (IOException e) {
            logger.error(key, e);
        }
        return null;
    }

    @Override
    public Object cacheKey(K input) {
        return hasher.apply("" + input);
    }

    @Override
    public Object cacheKey(Object... inputs) {
        return hasher.apply(Joiner.on('_').join(inputs));
    }

    @Override
    public void setHashFunc(HashFunction hasher) {
        if (null != hasher) {
            this.hasher = hasher;
        }
    }

    @Override
    public void replaceHashFunc(HashFunction hasher) {
        this.hasher = hasher;
    }
}
