package com.yeahmobi.datasystem.guice;
/**
 * Created by yangxu on 5/16/14.
 */

import com.google.inject.*;

public class TestProviderModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(String.class).toProvider(TestProvider.class).in(Singleton.class);
    }

    public static void main(String[] args) {
        Injector injector = Guice.createInjector(new TestProviderModule());
        String x = injector.getInstance(String.class);
        System.err.println(x);
        String y = injector.getInstance(String.class);
        System.err.println(y);

    }
}
