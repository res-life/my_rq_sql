package com.yeahmobi.datasystem.query.assist;

import io.druid.jackson.DefaultObjectMapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;

import org.apache.log4j.Logger;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import com.google.common.io.Closeables;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.yeahmobi.datasystem.query.akka.QueryConfig;
import com.yeahmobi.datasystem.query.akka.QueryConfigModule;
import com.yeahmobi.datasystem.query.akka.cache.CacheTool;
import com.yeahmobi.datasystem.query.akka.cache.QueryCacheFactory;
import com.yeahmobi.datasystem.query.akka.cache.XchangeRateCacheToolChest;
import com.yeahmobi.datasystem.query.meta.ReportContext;
import com.yeahmobi.datasystem.query.meta.ReportResult;
import com.yeahmobi.datasystem.query.process.PostProcessor;
import com.yeahmobi.datasystem.query.process.PostProcessorFactory;
import com.yeahmobi.datasystem.query.process.PreProcessor;
import com.yeahmobi.datasystem.query.process.QueryContext;
import com.yeahmobi.datasystem.query.process.XchangeProcessor;
import com.yeahmobi.datasystem.query.utils.Utils;

/**
 * <p>druid查询请求辅助类：预处理，后处理</p>
 * @since V1.0
 * @Author Martin
 * @createTime 2014年5月30日 下午1:04:59
 * @modifiedBy name
 * @modifyOn dateTime
 */
public class ProcessService {
    private static Logger logger = Logger.getLogger(ProcessService.class);
    
    public static String preProcess(String queryString) {
        String after = PreProcessor.process(queryString);
        if (logger.isDebugEnabled()) {
            logger.debug("before PreProcessing : " + queryString);
            logger.debug("after PreProcessing: " + after);
        }
        return after;
    }
    
    
    public static ReportResult postProcess(InputStream inputStream, ReportContext reportContext) {
        ReportResult reportResult = null;

        ObjectMapper objectMapper = new DefaultObjectMapper();
        QueryContext ctx = reportContext.getQueryContext();
        
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));

        // 后处理阶段
        Object res = null;
        try {
            res = objectMapper.readValue(bufferedReader, ctx.getTypeRef());
        } catch (JsonParseException e) {
            e.printStackTrace();
        } catch (JsonMappingException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        
        List<Object> ret = (List<Object>) res;
        PostProcessor postProcessor = PostProcessorFactory.create(ctx.getQueryType(), reportContext.getDruidReportParser());
        
        String queryData = ctx.getQueryParam();
        if (XchangeRateCacheHelper.xchangeIsEnabled() 
                && (!Strings.isNullOrEmpty(Utils.getCurrencyTypeFromQuery(queryData))) 
                && XchangeRateCacheHelper.paramContainsCurrency(Utils.getNodeXxxContents(queryData, Utils.DIM_SEG))) {
            Injector injector = Guice.createInjector(new QueryConfigModule());
            QueryConfig cfg = injector.getInstance(QueryConfig.class);
            CacheTool xchangeCacheTool = new XchangeRateCacheToolChest(QueryCacheFactory.create(cfg.getXchangeCacheType()), cfg.getXchangeCacheTtlFunc());
            
            reportResult = new XchangeProcessor(postProcessor.process(ret), queryData, xchangeCacheTool).exchange();
        }else {
            reportResult = postProcessor.process(ret);
        }
        
        
        Closeables.closeQuietly(bufferedReader);
        
        if (inputStream != null) {
            try {
                inputStream.close();
            } catch (IOException e) {
                logger.error("", e);
            }
        }
        
        return reportResult;
    }
    
//    public static ReportResult postProcess(InputStream inputStream, DruidReportParser parser) {
//        ReportResult reportResult = null;
//        
//        ObjectMapper objectMapper = new DefaultObjectMapper();
//        QueryContext ctx = QueryFactory.create(parser);
//        
//        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
//        
//        // 后处理阶段
//        Object res = null;
//        try {
//            res = objectMapper.readValue(bufferedReader, ctx.getTypeRef());
//        } catch (JsonParseException e) {
//            e.printStackTrace();
//        } catch (JsonMappingException e) {
//            e.printStackTrace();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        PostProcessor postProcessor = PostProcessorFactory.create(ctx.getQueryType(), parser, res);
//        reportResult = postProcessor.process();
//        
//        Closeables.closeQuietly(bufferedReader);
//        
//        return reportResult;
//    }
}
