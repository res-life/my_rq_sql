package com.yeahmobi.datasystem.config;
/**
 * Created by yangxu on 5/7/14.
 */

import com.google.inject.Binder;
import com.google.inject.Module;
import org.apache.log4j.Logger;

public class TestModule implements Module {

    private static Logger logger = Logger.getLogger(TestModule.class);

    public static class Foo {

    }

    @Override
    public void configure(Binder binder) {

        binder.bind(Foo.class).toInstance(new Foo());
    }

}
