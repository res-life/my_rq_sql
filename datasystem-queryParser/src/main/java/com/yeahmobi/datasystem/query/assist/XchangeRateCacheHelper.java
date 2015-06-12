package com.yeahmobi.datasystem.query.assist;

import java.io.IOException;
import java.util.Calendar;
import java.util.List;

import org.apache.log4j.Logger;

import com.google.common.base.Strings;
import com.google.common.collect.Sets;
import com.google.common.collect.Sets.SetView;
import com.google.gson.Gson;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.yeahmobi.datasystem.query.akka.QueryConfig;
import com.yeahmobi.datasystem.query.akka.QueryConfigModule;
import com.yeahmobi.datasystem.query.akka.cache.CacheTool;
import com.yeahmobi.datasystem.query.exception.ReportParserException;
import com.yeahmobi.datasystem.query.meta.XCHANGE_RATE_BASE;

public class XchangeRateCacheHelper {
    private static Logger logger = Logger.getLogger(XchangeRateCacheHelper.class);
    
    public static boolean hasXchangeMetric(List<String> data) {
        Injector injector = Guice.createInjector(new QueryConfigModule());
        QueryConfig cfg = injector.getInstance(QueryConfig.class);
        SetView<String> setView = Sets.intersection(Sets.newHashSet(cfg.getXchangeableAggMetrics().split(",")), Sets.newHashSet(data));
        return (null != setView && setView.size() > 0);
    }
    
    public static boolean xchangeIsEnabled() {
        Injector injector = Guice.createInjector(new QueryConfigModule());
        QueryConfig cfg = injector.getInstance(QueryConfig.class);
        boolean isEnabled = false;
        if (!Strings.isNullOrEmpty(cfg.getXchangeEnable())) {
            isEnabled = "enable".equalsIgnoreCase(cfg.getXchangeEnable());
        }
        return isEnabled;
    }
    public static boolean paramContainsCurrency(String dataContents) {
        return dataContents.contains("currency");
    }
    public static boolean updateCache(CacheTool cacheTool) {
        Injector injector = Guice.createInjector(new QueryConfigModule());
        QueryConfig cfg = injector.getInstance(QueryConfig.class);
        boolean result = false;
        List<XCHANGE_RATE_BASE> xchange_rate_bases = null;
        if (null != cacheTool) {
            try {
                xchange_rate_bases = NetService.getXchangeRateList();
            } catch (ReportParserException | IOException e) {
                logger.error("", e);
            }
            if (null != xchange_rate_bases && xchange_rate_bases.size() > 0) {
                for (XCHANGE_RATE_BASE xchange_rate_base : xchange_rate_bases) {
                    // cacheTool.set(from,to,value)
                    cacheTool.set(xchange_rate_base.getCurrency_from(), xchange_rate_base.getCurrency_to(), xchange_rate_base);
                    if (logger.isDebugEnabled()) {
                        String key = "xr-" + String.valueOf(xchange_rate_base.getCurrency_from()) + String.valueOf(xchange_rate_base.getCurrency_to());
                        logger.debug("Cached Add: key=" + key + ", ttl=" +  cfg.getXchangeCacheTtlFunc().apply(key) +"s, value="+ new Gson().toJson(xchange_rate_base));
                    }
                }
                result = true;
            }
        }
        return result;
    }
}
