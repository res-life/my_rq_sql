package com.yeahmobi.datasystem.query.redis;
/**
 * Created by yangxu on 4/14/14.
 */

import com.google.common.base.Strings;
import org.apache.log4j.Logger;
import redis.clients.jedis.JedisPoolConfig;

import java.util.*;
import java.util.concurrent.TimeUnit;

public class HAJedis implements JedisClient {

    private static Logger logger = Logger.getLogger(HAJedis.class);

    // Client candidates list
    List<HAJedisClient> clients = null;

    // healthy client ready to be used
    HAJedisClient client = null;

    String tag;
    int checkInterval;

    /**
     * constructor
     * @param tag: instance tag
     * @param hostPorts: hostPorts list: 127.0.0.1:6378,127.0.0.1:6379
     * @param ranks: rank list: 1,2, a master's rank is always greater than a slave
     * @param passwd: passwd for auth
     */
    HAJedis(String tag,
            String[] hostPorts, String[] ranks,
            String passwd, int checkInterval) {
        this.tag = tag;
        this.checkInterval = checkInterval;

        if (null != hostPorts && hostPorts.length != 0) {

            clients = new ArrayList<HAJedisClient>();
            for (int i = 0; i < hostPorts.length; ++i) {
                String hostPort = hostPorts[i];
                if (Strings.isNullOrEmpty(hostPort)) {
                    continue;
                }
                String[] vals = hostPort.split(":");
                String host = vals[0];
                if (Strings.isNullOrEmpty(host)) {
                    continue;
                }
                int port = tryParseInt(vals[1], 6379);
                int rank = (i < ranks.length ? tryParseInt(ranks[i], 2) : 2);

                JedisPoolConfig config = new JedisPoolConfig();
                config.setMaxActive(250);
                config.setMaxIdle(200);
                config.setMaxWait(1000);
                logger.info("init HAJedis with tag \"" + tag + "\":" +
                    rank + "," + host + "," + port + "," + passwd);
                clients.add(new HAJedisClient(rank, host, port, passwd, config));
            }

        }

        if (null == clients || clients.isEmpty()) {
            logger.error("init HAJedis with tag \"" + tag + "\" failed, pls check the config file");
        } else {
            // sort by rank
            Collections.sort(clients, new Comparator<HAJedisClient>() {
                @Override
                public int compare(HAJedisClient o1, HAJedisClient o2) {
                    if (null == o1) return 1;
                    if (null == o2) return -1;
                    return (o2.getRank() - o1.getRank());
                }
            });

//            Preconditions.checkState(getFirstAlive(), "No alive redis instance found");
            if(!getFirstAlive()) {
                logger.error("No alive redis instance found");
            }

            Thread sentinelThread = new Thread(new Sentinel(this), "HAJedis-Sentinel");
            sentinelThread.start();

        }
    }

    HAJedis(HAJedisConfig config) {
        this(config.tag,
                config.hostPorts,
                config.ranks,
                config.password,
                config.checkInterval
        );
    }


    @Override
    public List<String> getValues(String[] keys) {
        if (null != client) {
            return client.getValues(keys);
        }
        return null;
    }

    @Override
    public List<String> getValues(List<String> keys) {
        if (null != client) {
            return client.getValues(keys);
        }
        return null;
    }

    @Override
    public List<String> getValues(Set<String> keys) {
        if (null != client) {
            return client.getValues(keys);
        }
        return null;
    }

    @Override
    public String getOneValue(List<String> keys) {
        if (null != client) {
            return client.getOneValue(keys);
        }
        return null;
    }

    @Override
    public String getValue(String key) {
        if (null != client) {
            return client.getValue(key);
        }
        return null;
    }

    @Override
    public boolean setValue(String key, String value) {
        if (null != client) {
            return client.setValue(key, value);
        }
        return false;
    }

    @Override
    public boolean setValue(String key, String value, int ttl) {
        if (null != client) {
            return client.setValue(key, value, ttl);
        }
        return false;
    }

    @Override
    public boolean setValues(Map<String, String> pairs) {
        if (null != client) {
            return client.setValues(pairs);
        }
        return false;
    }

    @Override
    public boolean setValues(Map<String, String> pairs, int ttl) {
        if (null != client) {
            return client.setValues(pairs, ttl);
        }
        return false;
    }

    @Override
    public Set<String> keys(String prefix) {
        if (null != client) {
            return client.keys(prefix);
        }
        return null;
    }

    @Override
    public Long del(String key) {
        if (null != client) {
            return client.del(key);
        }
        return null;
    }

    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("HAJedis:[").append("tag:").append(tag);
        stringBuilder.append(",").append("clients:[");
        for (JedisClient client : clients) {
            if (null != client) {
                stringBuilder.append(client).append(",");
            }
        }
        stringBuilder.setCharAt(stringBuilder.length() - 1, ']');
        if (null != client) {
            stringBuilder.append(",client:").append(client.status);
        }
        stringBuilder.append(",checkInterval:").append(checkInterval)
        .append(']');

        return stringBuilder.toString();

    }

    private int i; // index of client to check
    boolean getFirstAlive() {
        if (null != clients && !clients.isEmpty()) {
            for (i = 0; i < clients.size(); ++i) {
                HAJedisClient client = clients.get(i);
                if (!client.isHealthy()) {
                    logger.error(tag + " --> " + client);
                } else { // pick first client available
                    if (this.client != client) {
                        logger.info(this.tag + "-- switch client: " + this.client + " --> " + client);
                        this.client = client;
                    }
                    break;
                }
            }
            if (null != this.client && this.client.isHealthy()) {
                return true;
            }
        }

        this.client = null;
        logger.error(this.tag + ": no alive client found\n");
        return false;
    }

    void check() {
        if (null != clients && !clients.isEmpty()) {
            for (; i < clients.size(); ++i) {
                HAJedisClient client = clients.get(i);
                if (!client.isHealthy()) {
                    logger.error(tag + " --> " + client);
                }
            }
        }
    }

    int tryParseInt(String str, int defVal) {
        if (!Strings.isNullOrEmpty(str)) {
            try {
                return Integer.parseInt(str);
            } catch (NumberFormatException e) {
            }
        }
        return defVal;
    }
}

class Sentinel implements Runnable {
    private final Logger logger = Logger.getLogger(Sentinel.class);

    HAJedis haJedis = null;
    private int checkInterval = 100;

    boolean running = false;

    Sentinel(HAJedis haJedis) {
        if (null != haJedis) {
            this.haJedis = haJedis;
            running = (null != haJedis.clients
                    && !haJedis.clients.isEmpty());

            this.checkInterval = haJedis.checkInterval;
        }
    }

    @Override
    public void run() {
        while (running) {
            if (null != haJedis) {
                haJedis.getFirstAlive();
                haJedis.check();
            }

            try {
                TimeUnit.MILLISECONDS.sleep(checkInterval);
            } catch (InterruptedException e) {
                logger.error("", e);
            }
        }
    }
}