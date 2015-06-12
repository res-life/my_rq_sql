package com.yeahmobi.datasystem.query.redis;
/**
 * Created by yangxu on 5/7/14.
 */

import com.google.inject.Binder;
import com.google.inject.Module;
import com.yeahmobi.datasystem.query.guice.JsonConfigurator;

import javax.validation.Validation;
import javax.validation.Validator;

public class HAJedisConfigModule implements Module {

    @Override
    public void configure(Binder binder) {
        binder.bind(Validator.class).toInstance(Validation.buildDefaultValidatorFactory().getValidator());
        binder.bind(JsonConfigurator.class);
    }
}
