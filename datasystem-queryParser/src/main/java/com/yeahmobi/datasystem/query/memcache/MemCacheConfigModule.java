package com.yeahmobi.datasystem.query.memcache;
/**
 * Created by yangxu on 5/17/14.
 */

import com.google.inject.Binder;
import com.google.inject.Module;
import com.yeahmobi.datasystem.query.guice.JsonConfigurator;

import javax.validation.Validation;
import javax.validation.Validator;

public class MemCacheConfigModule implements Module {

    @Override
    public void configure(Binder binder) {
        binder.bind(Validator.class).toInstance(Validation.buildDefaultValidatorFactory().getValidator());
        binder.bind(JsonConfigurator.class);
    }
}
