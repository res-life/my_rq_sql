package com.yeahmobi.datasystem.query.process;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.collect.Sets.SetView;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.yeahmobi.datasystem.query.akka.QueryConfig;
import com.yeahmobi.datasystem.query.akka.QueryConfigModule;
import com.yeahmobi.datasystem.query.akka.cache.CacheTool;
import com.yeahmobi.datasystem.query.akka.cache.QueryCacheFactory;
import com.yeahmobi.datasystem.query.akka.cache.XchangeRateCacheToolChest;
import com.yeahmobi.datasystem.query.assist.XchangeRateCacheHelper;
import com.yeahmobi.datasystem.query.meta.ReportResult;
import com.yeahmobi.datasystem.query.meta.ReportResult.Entity;
import com.yeahmobi.datasystem.query.meta.XCHANGE_RATE_BASE;







import com.yeahmobi.datasystem.query.utils.Utils;
import com.yeahmobi.datasystem.query.utils.YeahmobiUtils;











//import com.yeahmobi.datasystem.query.meta.ReportResult;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

//import static com.yeahmobi.datasystem.query.meta.ReportResult.*;

public class XchangeProcessor {
    private Logger logger = Logger.getLogger(XchangeProcessor.class);
    
    final ReportResult reportResult;
    final String currencyType;
    final String queryData;
    final CacheTool cacheTool;

    public XchangeProcessor(ReportResult reportResult, 
            //String currencyType, 
            String queryData, 
            CacheTool cacheTool) {
        this.reportResult = reportResult;
        // this.currencyType = currencyType;
        this.queryData = queryData;
        this.cacheTool = cacheTool;
        this.currencyType = Utils.getCurrencyTypeFromQuery(queryData);
    }

    public ReportResult exchange() {
        Injector injector = Guice.createInjector(new QueryConfigModule());
        QueryConfig cfg = injector.getInstance(QueryConfig.class);
        
        
        // HashSet<String> exchangeableAggMetrics = Sets.newHashSet("cost","revenue","profit","epc","rpc","cpc","arpa","acpa");
        HashSet<String> exchangeableAggMetrics = Sets.newHashSet(cfg.getXchangeableAggMetrics().split(","));
        HashMap<String, Integer> aggMetricsMap = null;
        ReportResult reportResult = null;
        Entity data = null;
        List<Object[]> datasArrList = null;
        if (null != this.reportResult) {
            reportResult = this.reportResult;
            data = reportResult.getData();
            datasArrList = data.getData();
            
            // mapping 辅助数据列
            String aggMetrics = YeahmobiUtils.rmWhiteSpace(Utils.getNodeXxxContents(this.queryData, Utils.DATA_SEG)).replace(Utils.PUNCTUATION_QUOTE, "");
            String dimensions = YeahmobiUtils.rmWhiteSpace(Utils.getNodeXxxContents(this.queryData, Utils.DIM_SEG)).replace(Utils.PUNCTUATION_QUOTE, "");
            Set<String> datasHeader_qry = Sets.newHashSet(Splitter.on(",").split(Joiner.on(",").join(dimensions, aggMetrics)));
            String rslt_datas = YeahmobiUtils.rmWhiteSpace(Joiner.on(",").join(datasArrList.get(0)));
            List<String> datasHeader_rslt = Lists.newArrayList(Splitter.on(",").split(rslt_datas));
            SetView<String> additionalAggMetrics = Sets.difference(Sets.newHashSet(datasHeader_rslt), datasHeader_qry);
            Map<String, Integer> diffMap = Maps.newHashMap();
            for (int i = 0; i < datasHeader_rslt.size(); i++) {
                String aggMetric = datasHeader_rslt.get(i);
                if (additionalAggMetrics.contains(aggMetric)) {
                    diffMap.put(aggMetric, i);
                }
            }
            
//            if (null != datasArrList && datasArrList.size() > 1 && (datasArrList.get(0) instanceof String[])) {
            if (null != datasArrList && (datasArrList.get(0) instanceof String[])) {
                List<String> tableHeaderList = Arrays.asList((String[]) datasArrList.get(0));
                SetView<String> tableHeaderSV = Sets.intersection(exchangeableAggMetrics, Sets.newHashSet(tableHeaderList));
                if (tableHeaderSV.size() > 0) {// 当结果集中的数据包含需要做汇率换算的数据列时，保存这些数据列的索引位置
                    aggMetricsMap = new HashMap<String, Integer>();
                    for (int i = 0; i < tableHeaderList.size(); i++) {
                        String metric = tableHeaderList.get(i);
                        if (tableHeaderSV.contains(metric) || "currency".equalsIgnoreCase(metric)) {
                            aggMetricsMap.put(metric, i);
                        }
                    }
                }
                if (aggMetricsMap.size() > 0) {
                    XCHANGE_RATE_BASE xrb = null;
                    // 重新计算需要做汇率换算的指标
                    for (int i = 0; i < datasArrList.size(); i++) {
                        // 获取结果集中的一条数据
                        Object[] datas = datasArrList.get(i);
                        if (i >= 1) { // (skip over 表头)获取数据值
                            // 根据查询请求指定的货币类型 currency_type 和 数据记录中的 currency 值，以‘currency+currency_type(CURRENCY_FROM+CURRENCY_TO)’为键获取缓存中的汇率换算表记录
                            for (Entry<String, Integer> entry: aggMetricsMap.entrySet()) {
                                String key = entry.getKey();
                                int index = entry.getValue();
                                if ("currency".equalsIgnoreCase(key)) {
                                    xrb = (XCHANGE_RATE_BASE) cacheTool.get(new XCHANGE_RATE_BASE(String.valueOf(datas[index]), this.currencyType, null, null), XCHANGE_RATE_BASE.class);
                                    if (null == xrb && XchangeRateCacheHelper.updateCache(cacheTool)) {
                                        xrb = (XCHANGE_RATE_BASE) cacheTool.get(new XCHANGE_RATE_BASE(String.valueOf(datas[index]), this.currencyType, null, null), XCHANGE_RATE_BASE.class);
                                        logger.info("Updated Cache : XchangeRateCacheType[" + cfg.getXchangeCacheType() +  "],XchangeRateCacheTTL[" + cfg.getXchangeCacheTtlFunc().apply() + "].");
                                    }
                                }
                            }
                            
                            if (null != xrb) {
                                // 分别对以下8种需要做汇率换算的指标进行重新计算、赋值
                                for (Entry<String, Integer> entry: aggMetricsMap.entrySet()) {
                                    String key = entry.getKey();
                                    int index = entry.getValue();
                                    switch (key) {
                                        case "cost":
                                            datas[index] = Xchangers.cost.apply(xrb, datas[index]);
                                            break;
                                        case "revenue":
                                            datas[index] = Xchangers.revenue.apply(xrb, datas[index]);
                                            break;
                                        case "profit":
                                            int rvn_index = aggMetricsMap.get("revenue");
                                            int cst_index = aggMetricsMap.get("cost");
                                            int prft_index = aggMetricsMap.get("profit");
                                            datas[index] = Xchangers.profit.apply(xrb, datas[rvn_index], datas[cst_index], datas[prft_index]);
                                            break;
                                        case "epc":
                                            datas[index] = Xchangers.epc.apply(xrb, datas[index]);
                                            break;
                                        case "rpc":
                                            datas[index] = Xchangers.rpc.apply(xrb, datas[index]);
                                            break;
                                        case "cpc":
                                            datas[index] = Xchangers.cpc.apply(xrb, datas[index]);
                                            break;
                                        case "arpa":
                                            datas[index] = Xchangers.arpa.apply(xrb, datas[index]);
                                            break;
                                        case "acpa":
                                            datas[index] = Xchangers.acpa.apply(xrb, datas[index]);
                                            break;
                                        default:
                                            break;
                                    }
                                }
                            }
                        }
                        
                        /*如果有，则剔除辅助数据列(否则，将直接)将最新的 结果数据放回原位置*/
                        if (diffMap.size() > 0) {
                            for (Entry<String, Integer> entry : diffMap.entrySet()) {
                                String key = entry.getKey();
                                int index = entry.getValue();
                                datas[index] = null;
                            }
                            String tmp = Joiner.on(",").skipNulls().join(datas);
                            datasArrList.set(i, tmp.split(","));
                        }else {
                            datasArrList.set(i, datas);
                        }
                    }
                }
                data.setData(datasArrList);
                reportResult.setData(data);
            }
        }
        return reportResult;
    }

    public static void main(String[] args) throws JsonParseException, JsonMappingException, UnsupportedEncodingException, IOException {
        // 集合的合集，交集，差集
        HashSet<Integer> setA = Sets.newHashSet(1, 2, 3, 4, 5);
        HashSet<Integer> setB = Sets.newHashSet(4, 5, 6, 7, 8);

        SetView<Integer> union = Sets.union(setA, setB);
        System.out.println("***********union***********");
        for (Integer integer : union)
            System.out.print(integer);
        System.out.println();
        SetView<Integer> difference = Sets.difference(setA, setB);
        System.out.println("***********difference***********");
        for (Integer integer : difference)
            System.out.print(integer);
        System.out.println();
        SetView<Integer> intersection = Sets.intersection(setA, setB);
        System.out.println("***********intersection***********");
        for (Integer integer : intersection)
            System.out.print(integer);
        System.out.println();
        
        XCHANGE_RATE_BASE xchange_rate_base = new XCHANGE_RATE_BASE();
        xchange_rate_base.setCurrency_from("CNY");
        xchange_rate_base.setCurrency_to("USD");
        xchange_rate_base.setRate_from_to(2.5);
        xchange_rate_base.setRate_usd_to(4.44);
        
        Object[] nums = {2.5,"2.5"};
        
        System.out.println(Xchangers.cost.apply(nums[0], xchange_rate_base));
        System.out.println(Xchangers.revenue.apply(nums[1], xchange_rate_base));
        
        Injector injector = Guice.createInjector(new QueryConfigModule());
        QueryConfig cfg = injector.getInstance(QueryConfig.class);
        System.out.println(cfg.getXchangeableAggMetrics());
        
        System.out.println("*************************");
        System.out.println(XchangeRateCacheHelper.xchangeIsEnabled());
        String nn = "http://localhost:8080/datasystem-realquery/report?report_param={%22settings%22:{%22report_id%22:%221402919015%22,%22return_format%22:%22json%22,%22time%22:{%22start%22:1402876800,%22end%22:1404172799,%22timezone%22:0},%22data_source%22:%22ymds_druid_datasource%22,%22pagination%22:{%22size%22:100000,%22page%22:0}},%22filters%22:{%22$and%22:{}},%22data%22:[%22rows%22],%22group%22:[%22transaction_id%22,%22day%22,%22currency%22],%22currency_type%22:%22USD%22}";
        try {
            System.out.println(URLDecoder.decode(nn, "UTF-8"));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        
        CacheTool xchangeCacheTool = new XchangeRateCacheToolChest(QueryCacheFactory.create(cfg.getXchangeCacheType()), cfg.getXchangeCacheTtlFunc());
        Object[] datas = {"65536", "de8a2cd9-570d-467a-abb6-5b7ea7a6fad5", 8, "GBP", 1, 1, 1, 2, 20.000};
        XCHANGE_RATE_BASE xrb = (XCHANGE_RATE_BASE) xchangeCacheTool.get(new XCHANGE_RATE_BASE(String.valueOf(datas[3]), "CNY", null, null), XCHANGE_RATE_BASE.class);
        if (null != xrb) {
            System.out.println(xrb.getRate_from_to());
            System.out.println(xrb.getRate_usd_to());
        }else {
            System.out.println("none get from cache.");
        }
        
        String ss = "{\"currency_from\":\"GBP\",\"currency_to\":\"CNY\",\"rate_from_to\":10.6318,\"rate_usd_to\":6.1995}";
        ObjectMapper mapper = new ObjectMapper();
        XCHANGE_RATE_BASE xchange_rate_base2 = mapper.readValue(ss.getBytes("UTF-8"), XCHANGE_RATE_BASE.class);
        System.out.println(xchange_rate_base2.getRate_usd_to());
    }
}
