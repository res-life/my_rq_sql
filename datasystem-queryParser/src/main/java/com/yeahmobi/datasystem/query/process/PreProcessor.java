package com.yeahmobi.datasystem.query.process;

/**
 * Created by yangxu on 3/17/14.
 */

import com.yeahmobi.datasystem.query.meta.DimensionTable;
import com.yeahmobi.datasystem.query.meta.TableDefaults;
import com.yeahmobi.datasystem.query.meta.TableRef;

import org.apache.log4j.Logger;

import com.google.common.base.CharMatcher;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.yeahmobi.datasystem.query.meta.MetricAggTable;
import com.yeahmobi.datasystem.query.skeleton.DataSourceViews;
import com.yeahmobi.datasystem.query.utils.YeahmobiUtils;

/**
 * 主要是处理前台请求中data里面的转化
 * 
 * @author yangxu
 * 
 */
public class PreProcessor {

    private final static Logger logger = Logger.getLogger(PreProcessor.class);

    private final static String DATA_SEG = "\"data\":";
    private final static String DIM_SEG = "\"group\":";

    public static String process(String query) {
        String res = query;
        String data_source = YeahmobiUtils.getDataSourceFromQuery(query);
        
        MetricAggTable metricAggTable = DataSourceViews.getViews().get(data_source).metrics().getMetricAggTable();
        res = doAlisa(res, DATA_SEG, metricAggTable, true);
        return res;
    }


    public static String doAlisa(String query, String tag,
                                 MetricAggTable metricAggTable,
                                 boolean recursion) {
        if (Strings.isNullOrEmpty(query))
            return query;
        String res = query;

        int index = query.indexOf(tag);
        if (-1 != index) {
            int s = query.indexOf("[", index) + 1; // skip '['
            int e = query.indexOf(']', s);
            if (-1 == e) {
                logger.error("invalid format:" + query);
                return null;
            }
            if (e != s) { // not empty
                Splitter splitter = Splitter.on(',').trimResults(CharMatcher.anyOf(" \""));
                Iterable<String> metrics = splitter.split(query.subSequence(s, e));
                StringBuilder builder = new StringBuilder();
                String eval;
                for (String metric : metrics) {
                    if (null != (eval = metricAggTable.getAggExpr(metric)))
                        builder.append(',').append(eval);
                    else
                        builder.append(',').append(metric);
                }
                if (builder.length() != 0) {
                    res = query.substring(0, s) + builder.substring(1) + query.substring(e);
                }
            }
        } else {
            if (recursion)
                doAlisa(res, "\"data\"", false);
        }

        return res;
    }

    public static String doAlisa(String query, String tag, boolean recursion) {
        if (Strings.isNullOrEmpty(query))
            return query;

        MetricAggTable metricAggTable = TableDefaults.metric;

        String res = query;

        int index = query.indexOf(tag);
        if (-1 != index) {
            int s = query.indexOf("[", index) + 1; // skip '['
            int e = query.indexOf(']', s);
            if (-1 == e) {
                logger.error("invalid format:" + query);
                return null;
            }
            if (e != s) { // not empty
                Splitter splitter = Splitter.on(',').trimResults(CharMatcher.anyOf(" \""));
                Iterable<String> metrics = splitter.split(query.subSequence(s, e));
                StringBuilder builder = new StringBuilder();
                String eval;
                for (String metric : metrics) {
                    if (null != (eval = metricAggTable.getAggExpr(metric)))
                        builder.append(',').append(eval);
                    else
                        builder.append(',').append(metric);
                }
                if (builder.length() != 0) {
                    res = query.substring(0, s) + builder.substring(1) + query.substring(e);
                }
            }
        } else {
            if (recursion)
                doAlisa(res, "\"data\"", false);
        }

        return res;
    }
}
