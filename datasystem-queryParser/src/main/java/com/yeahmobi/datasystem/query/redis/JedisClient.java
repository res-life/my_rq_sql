package com.yeahmobi.datasystem.query.redis;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by yangxu on 4/14/14.
 */

public interface JedisClient {

    /**
     * get corresponding values of given keys
     * @param keys: keys to query
     * @return values found, or null if exception
     */
    public List<String> getValues(String[] keys);

     /**
     * get corresponding values of given keys
     * @param keys: keys to query
     * @return values found, or null if exception
     */
    public List<String> getValues(List<String> keys);

    /**
     * get corresponding values of given keys
     * @param keys: keys to query
     * @return values found, or null if exception
     */
    public List<String> getValues(Set<String> keys);

    /**
     * get first available value of given keys
     * @param keys: keys to query
     * @return value found, or null if exception
     */
    public String getOneValue(List<String> keys);

    /**
     * get value of given key
     * @param key: key to query
     * @return value found, or null if exception
     */
    public String getValue(String key);

    /**
     * set value of given key
     * @param key: key to set
     * @param value: value of given key
     * @return true if success, or false if exception
     */
    public boolean setValue(String key, String value);

    /**
     * set value of given key
     * @param key: key to set
     * @param value: value of given key
     * @param ttl: ttl of give key [secs]
     * @return true if success, or false if exception
     */
    public boolean setValue(String key, String value, int ttl);

    /**
     * save a map of key-value pair into redis
     *
     * @param pairs : pairs to set
     * @return true if success, or false if exception
     */
    public boolean setValues(Map<String, String> pairs);

    /**
     * save a map of key-value pair into redis
     *
     * @param pairs : pairs to set
     * @param ttl : : ttl of give pairs
     * @return true if success, or false if exception
     */
    public boolean setValues(Map<String, String> pairs, int ttl);

    /**
     * get key set of given prefix
     * @param prefix: prefix of keys
     * @return true if success, or false if exception
     */
    public Set<String> keys(String prefix);

    /**
     * del record of given key in redis
     * @param key: : key to del
     * @return Integer reply if success, or null if exception
     */
    public Long del(String key);

}
