package com.yeahmobi.datasystem.query.akka;
/**
 * Created by yangxu on 5/16/14.
 */

import com.google.inject.*;
import com.yeahmobi.datasystem.query.guice.JsonConfigurator;

import javax.validation.Validation;
import javax.validation.Validator;

public class QueryConfigModule implements Module {

    @Override
    public void configure(Binder binder) {
        binder.bind(Validator.class).toInstance(Validation.buildDefaultValidatorFactory().getValidator());
        binder.bind(JsonConfigurator.class);
        binder.bind(QueryConfig.class).toProvider(QueryConfigProvider.class).in(Singleton.class);
    }
}
