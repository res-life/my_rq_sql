package com.yeahmobi.datasystem.query.pagination;
/**
 * Created by yangxu on 3/25/14.
 */

import org.apache.log4j.Logger;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class PageDataSource {

    private static Logger logger = Logger.getLogger(PageDataSource.class);

    String[] columns;
    boolean hasMore;
    int maxPageNum;

    ConcurrentHashMap<PageMeta, List<Object[]>> data = new ConcurrentHashMap<PageMeta, List<Object[]>>();

    List<Object[]> get(PageMeta pageMeta) {
        return data.get(pageMeta);
    }




}
