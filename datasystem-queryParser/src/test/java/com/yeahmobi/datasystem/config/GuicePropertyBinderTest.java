package com.yeahmobi.datasystem.config;

import java.util.Arrays;
import java.util.Properties;

import org.junit.Test;

import com.google.common.base.Strings;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.yeahmobi.datasystem.query.guice.JsonConfigurator;
import com.yeahmobi.datasystem.query.guice.PropertiesModule;
import com.yeahmobi.datasystem.query.memcache.MemCacheConfig;
import com.yeahmobi.datasystem.query.memcache.MemCacheConfigModule;

public class GuicePropertyBinderTest {
    private static final String CONFIG_BASE = "realquery.cache.memcache";
    @Test
    public void getProperty() {
        Injector injector = Guice.createInjector(new MemCacheConfigModule(), new PropertiesModule("config.properties"));

        JsonConfigurator jsonConfigurator = injector.getInstance(JsonConfigurator.class);

        MemCacheConfig cfg = jsonConfigurator.configurate(injector.getInstance(Properties.class), getKey(CONFIG_BASE, "query"), MemCacheConfig.class);
        System.out.println(Arrays.toString(cfg.getHostPorts()));
    }
    
    private static String getKey(String base, String suffix) {
        if (Strings.isNullOrEmpty(suffix)) return base;
        return base + "." + suffix;
    }
}
