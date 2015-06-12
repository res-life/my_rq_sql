package com.yeahmobi.datasystem.query.guice;
/**
 * Created by yangxu on 5/7/14.
 */

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.introspect.AnnotatedField;
import com.fasterxml.jackson.databind.introspect.BeanPropertyDefinition;
import com.google.common.base.Function;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import com.google.inject.ProvisionException;
import com.google.inject.spi.Message;
import org.apache.log4j.Logger;

import javax.annotation.Nullable;
import javax.validation.ConstraintViolation;
import javax.validation.ElementKind;
import javax.validation.Path;
import javax.validation.Validator;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.*;

public class JsonConfigurator {

    private static Logger logger = Logger.getLogger(JsonConfigurator.class);


    private final ObjectMapper jsonMapper;
    private final Validator validator;

    @Inject
    public JsonConfigurator(
            ObjectMapper jsonMapper,
            Validator validator
    )
    {
        this.jsonMapper = jsonMapper;
        this.validator = validator;
    }

    public <T> T configurate(Properties props, String propertyPrefix, Class<T> clazz) throws ProvisionException {
        verifyClazzIsConfigurable(clazz);

        // Make it end with a period so we only include properties with sub-object thingies.
        final String propertyBase = propertyPrefix.endsWith(".") ? propertyPrefix : propertyPrefix + ".";

        Map<String, Object> jsonMap = Maps.newHashMap();
        for (String prop : props.stringPropertyNames()) {
            if (prop.startsWith(propertyBase)) {
                final String propValue = props.getProperty(prop);
                Object value;
                try {
                    // If it's a String Jackson wants it to be quoted, so check if it's not an object or array and quote.
                    String modifiedPropValue = propValue;
                    if (! (modifiedPropValue.startsWith("[") || modifiedPropValue.startsWith("{"))) {
                        modifiedPropValue = String.format("\"%s\"", modifiedPropValue);
                    }
                    value = jsonMapper.readValue(modifiedPropValue, Object.class);
                }
                catch (IOException e) {
                    logger.error("Unable to parse [" + prop + "]=[" + propValue + "] as a json object, using as is.", e);
                    value = propValue;
                }

                jsonMap.put(prop.substring(propertyBase.length()), value);
            }
        }

        final T config;
        try {
            config = jsonMapper.convertValue(jsonMap, clazz);
        }
        catch (IllegalArgumentException e) {
            throw new ProvisionException(
                    String.format("Problem parsing object at prefix[%s]: %s.", propertyPrefix, e.getMessage()), e
            );
        }

        final Set<ConstraintViolation<T>> violations = validator.validate(config);
        if (!violations.isEmpty()) {
            List<String> messages = Lists.newArrayList();

            for (ConstraintViolation<T> violation : violations) {
                String path = "";
                try {
                    Class<?> beanClazz = violation.getRootBeanClass();
                    final Iterator<Path.Node> iter = violation.getPropertyPath().iterator();
                    while (iter.hasNext()) {
                        Path.Node next = iter.next();
                        if (next.getKind() == ElementKind.PROPERTY) {
                            final String fieldName = next.getName();
                            final Field theField = beanClazz.getDeclaredField(fieldName);

                            if (theField.getAnnotation(JacksonInject.class) != null) {
                                path = String.format(" -- Injected field[%s] not bound!?", fieldName);
                                break;
                            }

                            JsonProperty annotation = theField.getAnnotation(JsonProperty.class);
                            final boolean noAnnotationValue = annotation == null || Strings.isNullOrEmpty(annotation.value());
                            final String pathPart = noAnnotationValue ? fieldName : annotation.value();
                            if (path.isEmpty()) {
                                path += pathPart;
                            }
                            else {
                                path += "." + pathPart;
                            }
                        }
                    }
                }
                catch (NoSuchFieldException e) {
                    throw Throwables.propagate(e);
                }

                messages.add(String.format("%s - %s", path, violation.getMessage()));
            }

            throw new ProvisionException(
                    Iterables.transform(
                            messages,
                            new Function<String, Message>() {
                                @Nullable
                                @Override
                                public Message apply(@Nullable String input) {
                                    return new Message(String.format("%s%s", propertyBase, input));
                                }
                            }
                    )
            );
        }

        logger.info(String.format("Loaded class[%s] from props[%s] as [%s]", clazz, propertyBase, config));

        return config;
    }

    private <T> void verifyClazzIsConfigurable(Class<T> clazz)
    {
        final List<BeanPropertyDefinition> beanDefs = jsonMapper.getSerializationConfig()
                .introspect(jsonMapper.constructType(clazz))
                .findProperties();
        for (BeanPropertyDefinition beanDef : beanDefs) {
            final AnnotatedField field = beanDef.getField();
            if (field == null || !field.hasAnnotation(JsonProperty.class)) {
                throw new ProvisionException(
                        String.format(
                                "JsonConfigurator requires Jackson-annotated QueryConfig objects to have field annotations. %s doesn't",
                                clazz
                        )
                );
            }
        }
    }
}
