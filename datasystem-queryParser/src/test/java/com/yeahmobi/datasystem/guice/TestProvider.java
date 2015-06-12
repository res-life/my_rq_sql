package com.yeahmobi.datasystem.guice;

import com.google.inject.Provider;

/**
 * Created by yangxu on 5/16/14.
 */


public class TestProvider implements Provider<String> {

    @Override
    public String get() {
        System.out.println("TestProvider.get()");
        return "abc";
    }
}
