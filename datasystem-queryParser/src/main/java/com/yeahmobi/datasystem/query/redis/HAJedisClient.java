package com.yeahmobi.datasystem.query.redis;
/**
 * Created by yangxu on 4/14/14.
 */

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisDataException;

import java.util.*;

/**
 * high available Jedis client wrapper
 */
public class HAJedisClient implements JedisClient {

    private static Logger logger = Logger.getLogger(HAJedisClient.class);

    // connect options
    final private JedisPoolConfig config;
    final private JedisPool pool;

    // using priority of this client
    final private int rank;

    // auth info
    final private String host;
    final private int port;
    final private String passwd;

    // status of this client
    private DateTime startTime;
    private DateTime lastSeenTime;
    Status status;

    /**
     *
     * @param rank: using priority
     * @param host: host address of redis instance
     * @param port: port of redis instance
     * @param passwd: passwd used to auth connection
     * @param config: pool config used
     */
    HAJedisClient(int rank,
                  String host, int port, String passwd,
                  JedisPoolConfig config) {
        this.config = config;
        this.rank = rank;
        this.host = host;
        this.port = port;
        this.passwd = passwd;

        if (Strings.isNullOrEmpty(passwd)) {
            pool = new JedisPool(config, host, port, 10000);
        } else {
            pool = new JedisPool(config, host, port, 10000, passwd);
        }

        if (isHealthy()) {
            startTime = DateTime.now();
        }
    }

    /**
     * check if the redis client is still available
     * @return
     */
    boolean isHealthy() {
        if (null != pool) {
            Jedis jedis = null;
            try {
                jedis = getJedisResources();
                if ("PONG".equalsIgnoreCase(jedis.ping())) {
                    if (status != Status.Alive) {
                        startTime = DateTime.now();
                        status = Status.Alive;
                    }
                    lastSeenTime = DateTime.now();
                    returnResource(jedis);
                    return true;
                }
            } catch (JedisConnectionException e) {
                returnBrokenResource(jedis);
                logger.error("ping " + host + ":" + port + " failed", e);
            }

        }
        status = Status.Dead;
        return false;
    }


    /**
     * get up time since start
     * @return
     */
    long uptime() {
        return lastSeenTime.minus(startTime.getMillis()).getMillis();

    }

    public Jedis getJedisResources() throws JedisConnectionException {
        return pool.getResource();
    }

    public void returnBrokenResource(Jedis jedisClient) {
        pool.returnBrokenResource(jedisClient);
    }

    public void returnResource(Jedis jedisClient) {
        pool.returnResource(jedisClient);
    }


    public List<String> getValues(String[] keys) {
        if (null == keys || 0 == keys.length) {
            return null;
        }
        Jedis jedisClient = null;
        List<String> results = new ArrayList<String>();
        try {
            jedisClient = getJedisResources();
            for (String key : keys) {
                String result = jedisClient.get(key);
                results.add(result);
            }
            returnResource(jedisClient);
            return results;
        } catch (JedisConnectionException e) {
            logger.error("", e);
            returnBrokenResource(jedisClient);
        }
        return null;
    }

    @Override
    public List<String> getValues(List<String> keys) {
        if (null == keys || 0 == keys.size()) {
            return null;
        }
        Jedis jedisClient = null;
        List<String> results = new ArrayList<String>();
        try {
            jedisClient = getJedisResources();
            for (String key : keys) {
                String result = jedisClient.get(key);
                results.add(result);
            }
            returnResource(jedisClient);
            return results;
        } catch (JedisConnectionException e) {
            logger.error("", e);
            returnBrokenResource(jedisClient);
        }
        return null;
    }

    public List<String> getValues(Set<String> keys) {
        if (null == keys || keys.isEmpty()) {
            return null;
        }
        Jedis jedisClient = null;
        List<String> results = new ArrayList<String>();
        try {
            jedisClient = getJedisResources();
            Iterator it = keys.iterator();
            while (it.hasNext()) {
                String key = (String) it.next();
                String result = jedisClient.get(key);
                results.add(result);
            }
            returnResource(jedisClient);
            return results;
        } catch (JedisConnectionException e) {
            logger.error("", e);
            returnBrokenResource(jedisClient);
        }
        return null;
    }

    public String getOneValue(List<String> keys) {
        if (null == keys || 0 == keys.size()) {
            return null;
        }
        Jedis jedisClient = null;
        String result = null;
        try {
            jedisClient = getJedisResources();
            for (String key : keys) {
                result = jedisClient.get(key);
                if (!Strings.isNullOrEmpty(result)) {
                    break;
                }
            }
            returnResource(jedisClient);
            return result;
        } catch (JedisConnectionException e) {
            logger.error("", e);
            returnBrokenResource(jedisClient);
        }
        return null;
    }

    public String getValue(String key) {
        if (Strings.isNullOrEmpty(key)) {
            return null;
        }
        Jedis jedisClient = null;
        try {
            jedisClient = getJedisResources();
            String result = jedisClient.get(key);
            returnResource(jedisClient);
            return result;
        } catch (JedisConnectionException e) {
            logger.error(key, e);
            if (null != jedisClient) {
                returnBrokenResource(jedisClient);
            }
        }
        return null;
    }

    public boolean setValue(String key, String value) {
        Jedis jedisClient = null;
        Exception exception;
        try {
            jedisClient = getJedisResources();
            jedisClient.set(key, value);
            returnResource(jedisClient);
            return true;
        } catch (JedisConnectionException e) {
            exception = e;
        } catch (JedisDataException e) { // read only
            exception = e;
        }
        logger.error(key + "->" + value, exception);
        if (null != jedisClient) {
            returnBrokenResource(jedisClient);
        }
        return false;
    }


    public boolean setValue(String key, String value, int ttl) {
        Jedis jedisClient = null;
        Exception exception;
        try {
            jedisClient = getJedisResources();
            jedisClient.set(key, value);
            jedisClient.expire(key, ttl);
            returnResource(jedisClient);
            return true;
        } catch (JedisConnectionException e) {
            exception = e;
        } catch (JedisDataException e) { // read only
            exception = e;
        }
        logger.error(key + "->" + value, exception);
        if (null != jedisClient) {
            returnBrokenResource(jedisClient);
        }
        return false;
    }

    public boolean setValues(Map<String, String> pairs) {
        Jedis jedisClient = null;
        Exception exception;
        try {
            jedisClient = getJedisResources();
            for (Map.Entry<String, String> entry : pairs.entrySet()) {
                jedisClient.set(entry.getKey(), entry.getValue());
            }
            returnResource(jedisClient);
            return true;
        } catch (JedisConnectionException e) {
            exception = e;
        } catch (JedisDataException e) { // read only
            exception = e;
        }
        logger.error("keys: " + Joiner.on(", ").join(pairs.keySet()));
        logger.error("values: " + Joiner.on(", ").join(pairs.values()));
        logger.error("", exception);
        if (null != jedisClient) {
            returnBrokenResource(jedisClient);
        }
        return false;
    }


    public boolean setValues(Map<String, String> pairs, int ttl) {
        Jedis jedisClient = null;
        Exception exception;
        try {
            jedisClient = getJedisResources();
            for (Map.Entry<String, String> entry : pairs.entrySet()) {
                jedisClient.set(entry.getKey(), entry.getValue());
                jedisClient.expire(entry.getKey(), ttl);
            }
            returnResource(jedisClient);
            return true;
        } catch (JedisConnectionException e) {
            exception = e;
        } catch (JedisDataException e) { // read only
            exception = e;
        }
        logger.error("keys: " + Joiner.on(", ").join(pairs.keySet()));
        logger.error("values: " + Joiner.on(", ").join(pairs.values()));
        logger.error("", exception);
        if (null != jedisClient) {
            returnBrokenResource(jedisClient);
        }
        return false;
    }


    public Long del(String key) {
        if (Strings.isNullOrEmpty(key)) {
            return null;
        }
        Jedis jedisClient = null;
        Exception exception;
        try {
            jedisClient = getJedisResources();
            Long result = jedisClient.del(key);
            returnResource(jedisClient);
            return result;
        } catch (JedisConnectionException e) {
            exception = e;
        } catch (JedisDataException e) { // read only
            exception = e;
        }
        logger.error(key, exception);
        if (null != jedisClient) {
            returnBrokenResource(jedisClient);
        }
        return null;
    }

    public Set<String> keys(String prefix) {
        if (Strings.isNullOrEmpty(prefix)) {
            return null;
        }
        Jedis jedisClient = null;
        Set<String> keys = null;
        try {
            jedisClient = getJedisResources();
            keys = jedisClient.keys(prefix + "*");
            returnResource(jedisClient);
        } catch (JedisConnectionException e) {
            logger.error(prefix, e);
            if (null != jedisClient) {
                returnBrokenResource(jedisClient);
            }
        }
        return keys;
    }

    public int getRank() {
        return rank;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public DateTime getStartTime() {
        return startTime;
    }

    public DateTime getLastSeenTime() {
        return lastSeenTime;
    }

    public String toString() {
        return "HAJedisClient:["
                + "status=" + status + ",rank=" + rank
                + ",host=" + host + ",port=" + port
                + ",startTime=" + startTime
                + ",lastSeenTime=" + lastSeenTime
                + ",upTime=" + ((null == lastSeenTime) ? 0 : lastSeenTime.minus(startTime.getMillis()).getMinuteOfHour())
                + "]";

    }
}

enum Status {
    Alive,
    Dead
}
