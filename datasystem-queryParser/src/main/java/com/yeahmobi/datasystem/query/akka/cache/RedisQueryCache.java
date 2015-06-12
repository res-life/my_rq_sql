package com.yeahmobi.datasystem.query.akka.cache;
/**
 * Created by yangxu on 5/6/14.
 */

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.yeahmobi.datasystem.query.redis.JedisClient;
import com.yeahmobi.datasystem.query.redis.JedisClientFactory;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

public class RedisQueryCache<K, V> implements QueryCache<K, V> {

    private static Logger logger = Logger.getLogger(RedisQueryCache.class);

    private static RedisQueryCache instance = new RedisQueryCache();
    private RedisQueryCache() {

    }

    public static RedisQueryCache getInstance() {
        return instance;
    }

    private JedisClient jedisClient = JedisClientFactory.getInstance().get("query");

    private final static ObjectMapper mapper = new ObjectMapper();

    HashFunction hasher = UniqHashFunction.Md5;

    @Override
    public void set(K key, V object) {
        if (null == key || null == object) {
            return;
        }
        try {
            String val = mapper.writeValueAsString(object);
            jedisClient.setValue((String)cacheKey(key), val);
        } catch (JsonProcessingException e) {
            logger.error(key + "->" + object, e);
        }
    }

    @Override
    public void set(K key, V object, long ttl) {
        if (null == key || null == object) {
            return;
        }
        try {
            String val = mapper.writeValueAsString(object);
            jedisClient.setValue((String)cacheKey(key), val, (int)ttl);
        } catch (JsonProcessingException e) {
            logger.error(key + "->" + object, e);
        }
    }


    @Override
    public String get(K key) {
        return jedisClient.getValue((String)cacheKey(key));
    }

    @Override
    public V get(K key, Class<V> clazz) {
        try {
            String val = jedisClient.getValue((String)cacheKey(key));
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
