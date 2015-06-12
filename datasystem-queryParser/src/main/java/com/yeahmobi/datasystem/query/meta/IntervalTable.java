package com.yeahmobi.datasystem.query.meta;
/**
 * Created by yangxu on 3/17/14.
 */

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.base.CharMatcher;
import com.yeahmobi.datasystem.query.serializer.ObjectSerializer;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

final public class IntervalTable {

    private static Logger logger = Logger.getLogger(IntervalTable.class);


    private static Map<String, IntervalUnit> table;
    static {
        // dimensions
        // try read from file
        table = ObjectSerializer.read(IntervalTable.class.getSimpleName() + ".json",
                new TypeReference<Map<String, IntervalUnit>>() { });

        if (null == table) { // read from file error, load default
            table = new HashMap<String, IntervalUnit>();
            table.put("hour", IntervalUnit.HOUR);
            table.put("day", IntervalUnit.DAY);
            table.put("week", IntervalUnit.WEEKOFMONTH);
            table.put("month", IntervalUnit.MONTH);
            table.put("year", IntervalUnit.YEAR);
        }
        logger.info(ObjectSerializer.write(table).toString());
    }

    public static boolean contains(String interval) {
        return (null != table.get(CharMatcher.is('"').trimFrom(interval).toLowerCase()));
    }

    public static void persist() {

        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.enable(SerializationFeature.INDENT_OUTPUT);
        try {
            objectMapper.writeValue(new File(IntervalTable.class.getSimpleName() + ".json"), table);
        } catch (IOException e) {
            logger.error("", e);
        }
    }

    public static Map<String, IntervalUnit> getTable() {
        return table;
    }
}
