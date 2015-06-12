package com.yeahmobi.datasystem.query.serializer;

/**
 * Created by yangxu on 3/31/14.
 */

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.base.Strings;
import com.google.common.io.Closeables;
import org.apache.log4j.Logger;

import java.io.*;
import java.net.URL;
import java.nio.charset.Charset;

/**
 * 
 * 使用jackson方式进行json转化成java对象
 * 
 * @author yangxu
 * 
 */
public class ObjectSerializer {

    private static Logger logger = Logger.getLogger(ObjectSerializer.class);

    public static <T> T read(String name, TypeReference<T> typeReference) {
    	ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
    	return read(name,typeReference, classLoader);
    }

    public static <T> T read(String name, TypeReference<T> typeReference, ClassLoader classLoader) {

        if (!Strings.isNullOrEmpty(name)) {
            ObjectMapper objectMapper = new ObjectMapper();
            URL uri = classLoader.getResource(name);
            logger.info(null == uri ? " null " : uri.getPath());
            InputStream in = classLoader.getResourceAsStream(name);
            if (null != in) {
                try {
                    return objectMapper.readValue(in, typeReference);
                } catch (IOException e) {
                    logger.error("", e);
                } finally {
                    try {
                        in.close();
                    } catch (IOException e) {
                        logger.error("", e);
                    }
                }
            } else {
                logger.error("not found " + name);
            }
        }
        return null;
    }
    
    public static <T> T readObject(String name, JavaType valueType) {

        if (!Strings.isNullOrEmpty(name)) {
            ObjectMapper objectMapper = new ObjectMapper();
            ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
            URL uri = classLoader.getResource(name);
            // objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES,
            // false);
            logger.info(null == uri ? " null " : uri.getPath());
            InputStream in = classLoader.getResourceAsStream(name);
            BufferedReader brReader = new BufferedReader(new InputStreamReader(in, Charset.forName("UTF-8")));
            StringBuilder builder = new StringBuilder();
            String tempString = "";
            try {
                while ((tempString = brReader.readLine()) != null) {
                    builder.append(tempString);
                }
            } catch (IOException e1) {
                logger.error("", e1);
            }
            if (builder.length() != 0) {
                try {
                    return objectMapper.readValue(builder.toString(), valueType);
                } catch (IOException e) {
                    logger.error("", e);
                }
            } else {
                logger.error("not found " + name);
            }
            Closeables.closeQuietly(in);
        }
        return null;
    }

    public static JavaType getCollectionType(Class<?> collectionClass, Class<?>... elementClasses) {
        ObjectMapper objectMapper = new ObjectMapper();
        return objectMapper.getTypeFactory().constructParametricType(collectionClass, elementClasses);
    }

    public static <T> void write(String name, T object, ClassLoader loader) {

        if (null != object) {

            ObjectMapper objectMapper = new ObjectMapper();
            objectMapper.enable(SerializationFeature.INDENT_OUTPUT);

            String valName = name;
            if (Strings.isNullOrEmpty(valName)) {
                valName = object.getClass().getSimpleName() + ".json";
            }
            URL uri = loader.getResource(name);
            valName = uri.getPath();

            try {
                objectMapper.writeValue(new File(valName), object);
            } catch (IOException e) {
                logger.error("", e);
            }
        }
    }

    public static <T> void write(String name, T object) {

        if (null != object) {

            ObjectMapper objectMapper = new ObjectMapper();
            objectMapper.enable(SerializationFeature.INDENT_OUTPUT);

            String valName = name;
            if (Strings.isNullOrEmpty(valName)) {
                valName = object.getClass().getSimpleName() + ".json";
            }
            ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
            URL uri = classLoader.getResource(name);
            if (null != uri) {
                valName = uri.getPath() + "/" + valName;
            }

            try {
                objectMapper.writeValue(new File(valName), object);
            } catch (IOException e) {
                logger.error("", e);
            }
        }
    }
    
    public static <T> StringWriter write(T object) {

        StringWriter stringWriter = new StringWriter();
        if (null != object) {

            ObjectMapper objectMapper = new ObjectMapper();
            objectMapper.enable(SerializationFeature.INDENT_OUTPUT);

            try {
                objectMapper.writeValue(stringWriter, object);
            } catch (IOException e) {
                logger.error("", e);
            }
        }
        return stringWriter;
    }

    // public static void main(String[] args) {
    // ConfigManager.getInstance().init();
    // ObjectSerializer.write(QueryConfig.class.getSimpleName() + ".json",
    // ConfigManager.getInstance().getCfg());
    //
    // ObjectSerializer.write(DimensionTable.class.getSimpleName() + ".json",
    // DimensionTable.getTable());
    //
    // ObjectSerializer.write(MetricAggTable.class.getSimpleName() + ".json",
    // MetricAggTable.getTable());
    //
    // ObjectSerializer.write(IntervalTable.class.getSimpleName() + ".json",
    // IntervalTable.getTable());
    //
    // }
}
