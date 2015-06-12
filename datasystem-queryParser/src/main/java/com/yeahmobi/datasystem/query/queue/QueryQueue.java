package com.yeahmobi.datasystem.query.queue;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import com.google.gson.Gson;
import com.yeahmobi.datasystem.query.antlr4.DruidReportParser;
import com.yeahmobi.datasystem.query.meta.QueryEntry;
import com.yeahmobi.datasystem.query.skeleton.PostContext;
import com.yeahmobi.datasystem.query.skeleton.PostProcess;

public class QueryQueue {
    private static Logger logger = Logger.getLogger(QueryQueue.class);

    public static ArrayBlockingQueue<QueryEntry> queue = null;
    private static AtomicInteger count = new AtomicInteger();
    static {
        queue = new ArrayBlockingQueue<QueryEntry>(10);
    }

    private static QueryEntry queryEntry = null;

    public QueryQueue() {
    }

    public QueryQueue(QueryEntry queryEntry) {
        this.queryEntry = queryEntry;
    }

    public static void addQuery(PostProcess postProcess, String query, String url, boolean isImpala, String dataSource, DruidReportParser parser, String format, PostContext postContext) throws UnsupportedEncodingException {
        if (StringUtils.isNotBlank(query)) {
            queryEntry = new QueryEntry(postProcess, URLDecoder.decode(query, "UTF-8"), url, isImpala, dataSource, parser, format, postContext);
            // String queryString = new Gson().toJson(queryEntry);
            if (!queue.offer(queryEntry)) {
                logger.error("Query" + query + "Is Blocking. As QueryQueue Is Full...");
            } else {
                logger.info("Query" + query + "Has Been Put Into QueryQueue,Watting for Processing...");
            }
        }

    }

    public static void addQuery(QueryEntry queryEntry) {
        String queryString = new Gson().toJson(queryEntry);
        if (null != queryEntry) {
            if (!queue.offer(queryEntry)) {
                logger.error("Query" + queryString + "Is Blocking. As QueryQueue Is Full...");
            } else {
                logger.info("Query" + queryString + "Has Been Put Into QueryQueue，Watting for Processing...");
            }
        }
    }
}
