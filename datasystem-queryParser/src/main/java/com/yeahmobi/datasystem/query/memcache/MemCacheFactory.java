package com.yeahmobi.datasystem.query.memcache;
/**
 * Created by yangxu on 5/17/14.
 */

import com.google.common.base.Strings;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.yeahmobi.datasystem.query.guice.JsonConfigurator;
import com.yeahmobi.datasystem.query.guice.PropertiesModule;
import net.spy.memcached.AddrUtil;
import net.spy.memcached.MemcachedClient;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

public class MemCacheFactory {

    private static Logger logger = Logger.getLogger(MemCacheFactory.class);

    private Map<String, MemcachedClient> store = new ConcurrentHashMap<String, MemcachedClient>();

    private static final String CONFIG_BASE = "realquery.cache.memcache";

    private static String getKey(String base, String suffix) {
        if (Strings.isNullOrEmpty(suffix)) return base;
        return base + "." + suffix;
    }

    private static MemcachedClient init(String tag) {

        Injector injector = Guice.createInjector(new MemCacheConfigModule(),
                new PropertiesModule("config.properties"));

        JsonConfigurator jsonConfigurator = injector.getInstance(JsonConfigurator.class);

        MemCacheConfig cfg = jsonConfigurator.configurate(injector.getInstance(Properties.class), getKey(CONFIG_BASE, tag), MemCacheConfig.class);

        logger.info("Init memcache client with cfg : " + Arrays.toString(cfg.getHostPorts()) + ".");

        MemcachedClient client = null;
        try {
            client = new MemcachedClient(AddrUtil.getAddresses(Arrays.asList(cfg.getHostPorts())));
        } catch (IOException e) {
            logger.error("", e);
        }
        return client;

    }


    private static MemCacheFactory instance = new MemCacheFactory();
    private MemCacheFactory() {

    }

    public static MemCacheFactory getInstance() {
        return instance;
    }


    private Object lock = new Object();

    public MemcachedClient get(String valKey) {
        String key = Strings.nullToEmpty(valKey);
        MemcachedClient client = store.get(key);
        if (null == client) {
            synchronized (lock) {
                client = store.get(key);
                if (null == client) {
                    client = init(key);
                    if (null != client)
                        store.put(key, client);
                }
            }
        }
        return client;
    }

    public int nRedisPool() {
        return store.size();
    }

}
