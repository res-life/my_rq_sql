package com.yeahmobi.datasystem.query.redis;
/**
 * Created by yangxu on 2/22/14.
 */

import com.google.common.base.Strings;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.yeahmobi.datasystem.query.guice.JsonConfigurator;
import com.yeahmobi.datasystem.query.guice.PropertiesModule;
import org.apache.log4j.Logger;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

public class JedisClientFactory {

    private static Logger logger = Logger.getLogger(JedisClientFactory.class);

    private Map<String, JedisClient> store = new ConcurrentHashMap<String, JedisClient>();

    private static final String CONFIG_BASE = "realquery.cache.redis";

    private static String getKey(String base, String suffix) {
        if (Strings.isNullOrEmpty(suffix)) return base;
        return base + "." + suffix;
    }

    private static JedisClient init(String tag) {

        Injector injector = Guice.createInjector(new HAJedisConfigModule(),
                  new PropertiesModule("config.properties"));

        JsonConfigurator jsonConfigurator = injector.getInstance(JsonConfigurator.class);

        HAJedisConfig cfg = jsonConfigurator.configurate(injector.getInstance(Properties.class), getKey(CONFIG_BASE, tag), HAJedisConfig.class);

        logger.info("init HAJedis with " + cfg);
        return new HAJedis(cfg);

    }


    private static JedisClientFactory instance = new JedisClientFactory();
    private JedisClientFactory() {

    }

    public static JedisClientFactory getInstance() {
        return instance;
    }


    private Object lock = new Object();

    public JedisClient get(String valKey) {
        String key = Strings.nullToEmpty(valKey);
        JedisClient jedisClient = store.get(key);
        if (null == jedisClient) {
            synchronized (lock) {
                jedisClient = store.get(key);
                if (null == jedisClient) {
                    jedisClient = init(key);
                    store.put(key, jedisClient);
                }
            }
        }
        return jedisClient;
    }

    public int nRedisPool() {
        return store.size();
    }


}
