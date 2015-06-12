package com.yeahmobi.datasystem.query.akka.cache;

/**
 * Created by yangxu on 5/5/14.
 */

public interface QueryCache<K, V> {

    /**
     * cache object with key
     * @param key: cache key
     * @param object: object to cache
     */
    public void set(K key, V object);

     /**
     * cache object with key
     * @param key: cache key
     * @param object: object to cache
     * @param ttl: time to resident in cache
     */
    public void set(K key, V object, long ttl);

    /**
     * get object cache with key
     * @param key: cache key
     * @return
     */
    public String get(K key);

    /**
     * get object cache with key
     * @param key: cache key
     * @param clazz:
     * @return
     */
    public V get(K key, Class<V> clazz);

    /**
     *
     * @param input
     * @return
     */
    public Object cacheKey(K input);

     /**
     *
     * @param inputs
     * @return
     */
    public Object cacheKey(Object... inputs);

    public void setHashFunc(HashFunction hasher);

    public void replaceHashFunc(HashFunction hasher);
}
