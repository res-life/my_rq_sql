package com.yeahmobi.datasystem.config;
/**
 * Created by yangxu on 5/7/14.
 */

import com.google.inject.Binder;
import com.google.inject.Module;
import com.yeahmobi.datasystem.query.guice.JsonConfigurator;
import org.apache.log4j.Logger;

import javax.validation.Validation;
import javax.validation.Validator;

public class ConfigModule implements Module {

    private static Logger logger = Logger.getLogger(ConfigModule.class);

    @Override
    public void configure(Binder binder) {
        binder.bind(Validator.class).toInstance(Validation.buildDefaultValidatorFactory().getValidator());
        binder.bind(JsonConfigurator.class);
    }
}
