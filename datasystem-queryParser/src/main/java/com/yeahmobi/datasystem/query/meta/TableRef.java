package com.yeahmobi.datasystem.query.meta;
/**
 * Created by yangxu on 5/12/14.
 */

import org.apache.log4j.Logger;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;


public class TableRef {

    private static Logger logger = Logger.getLogger(TableRef.class);

    private final ConcurrentMap<String, TableSpec> tables = new ConcurrentHashMap<String, TableSpec>();
    private final Object lock = new Object();

    private static TableRef instance = new TableRef();

    public static TableRef getInstance() {return instance;}

    private TableRef() {}

    public TableSpec of(Class<? extends TableSpec> tClass, String ds) {

        String key = tClass.getCanonicalName() + "_" + ds;
        TableSpec table = tables.get(key);
        if (null == table) {
            synchronized (lock) {
                table = tables.get(key);
                if (null == table) {
                    try {
                        table = tClass.newInstance();
                        table.init(ds);
                    } catch (InstantiationException e) {
                        logger.error("", e);
                    } catch (IllegalAccessException e) {
                        logger.error("", e);
                    }
                    if (null == table) {
                        table = TableDefaults.of(tClass);
                    }
                    tables.put(key, table);
                }
            }
        }

        return table;
    }

}
