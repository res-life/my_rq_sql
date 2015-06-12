package com.yeahmobi.datasystem.config;
/**
 * Created by yangxu on 5/7/14.
 */

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.gson.Gson;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.yeahmobi.datasystem.query.guice.JsonConfigurator;
import com.yeahmobi.datasystem.query.guice.PropertiesModule;
import org.apache.log4j.Logger;

import java.util.Properties;

public class Test {

    private static Logger logger = Logger.getLogger(Test.class);

    public static void main(String[] args) throws JsonProcessingException {
        Injector injector = Guice.createInjector(new ConfigModule(),
                new PropertiesModule("runtime.properties"));

        JsonConfigurator jsonConfigurator = injector.getInstance(JsonConfigurator.class);

        Config cfg = jsonConfigurator.configurate(injector.getInstance(Properties.class), "test.config", Config.class);

        Properties p1 = injector.getInstance(Properties.class);
        Properties p2 = injector.getInstance(Properties.class);
        System.out.println(p1 == p2 ? "true" : "false");
        System.out.println(new Gson().toJson(cfg));

        TestModule.Foo foo1 = injector.getInstance(TestModule.Foo.class);
        TestModule.Foo foo2 = injector.getInstance(TestModule.Foo.class);

        // not signleton here
        System.out.println(foo1 == foo2 ? "true" : "false");
    }
}
