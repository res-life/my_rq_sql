package com.yeahmobi.datasystem.query.process;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.junit.Ignore;
import org.junit.Test;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.collect.Sets.SetView;
import com.google.gson.Gson;
import com.yeahmobi.datasystem.query.meta.ReportPage;
import com.yeahmobi.datasystem.query.meta.ReportResult;
import com.yeahmobi.datasystem.query.meta.ReportResult.Entity;
import com.yeahmobi.datasystem.query.utils.Utils;
import com.yeahmobi.datasystem.query.utils.YeahmobiUtils;

public class QueryProcessorTest {
    
//    String query_param = "{\"currency_type\":\"USD\",\"settings\":{\"report_id\":\"af3a0e3ea19a76157e5cd456b047d2c2\",\"return_format\":\"json\",\"data_source\":\"ymds_druid_datasource\",\"time\":{\"start\":1401811200,\"end\":1401983999,\"timezone\":0},\"pagination\":{\"size\":10000,\"page\":0}},\"group\":[\"click_time\",\"conv_time\",\"time_diff\",\"aff_id\",\"aff_sub1\",\"offer_id\",\"transaction_id\",\"ref_track\",\"country\",\"conv_ip\",\"click_ip\"],\"data\":[\"cr\",\"ecpc\",\"revenue\",\"profit\"],\"filters\":{\"$and\":{\"aff_manager\":{\"$in\":[\"90010453\"]}}}}";
//    String query_param = "{\"currency_type\":\"USD\",\"settings\":{\"report_id\":\"af3a0e3ea19a76157e5cd456b047d2c2\",\"return_format\":\"json\",\"data_source\":\"ymds_druid_datasource\",\"time\":{\"start\":1401811200,\"end\":1401983999,\"timezone\":0},\"pagination\":{\"size\":10000,\"page\":0}},\"group\":[\"click_time\",\"conv_time\",\"time_diff\",\"aff_id\",\"aff_sub1\",\"offer_id\",\"transaction_id\",\"ref_track\",\"country\",\"conv_ip\",\"click_ip\"],\"data\":[\"cr\",\"acpc\",\"cost\",\"profit\"],\"filters\":{\"$and\":{\"aff_manager\":{\"$in\":[\"90010453\"]}}}}";
    String query_param = "{\"currency_type\":\"USD\",\"settings\":{\"report_id\":\"af3a0e3ea19a76157e5cd456b047d2c2\",\"return_format\":\"json\",\"data_source\":\"ymds_druid_datasource\",\"time\":{\"start\":1401811200,\"end\":1401983999,\"timezone\":0},\"pagination\":{\"size\":10000,\"page\":0}},\"group\":[\"aff_id\",\" transaction_id\",\" itvl_hour\",\" currency\"],\"data\":[\"cr\",\"cost\",\"profit\"],\"filters\":{\"$and\":{\"aff_manager\":{\"$in\":[\"90010453\"]}}}}";
    
    @Test
    public void preProcessNeededTest() {
        String dataStr = "revenue,cost,profit";
        String q1dataStr = "revenue,cost,profit";
        String q2dataStr = "cost,profit";
        String q3dataStr = "cr,ecpc,revenue,profit";
        String q5dataStr = "cr,ecpc,profit";
        String q4dataStr = "revenue,cost";
        
        List<String> dataList = Lists.newArrayList(Splitter.on(",").split(dataStr));
        
        for (int i = 0; i < dataList.size(); i++) {
            dataList.set(i, "\"" + dataList.get(i) + "\"");
        }
        String queryData = String.format(query_param, (!Strings.isNullOrEmpty(Joiner.on(",").join(dataList))) ? ("," + Joiner.on(",").join(dataList)) : "");
        String dataContents = Utils.getNodeXxxContents(queryData, Utils.DATA_SEG);
        System.out.println(queryData);
        System.out.println(dataContents);
        // [group + data]
        
        Set<String> set = Sets.newHashSet(Splitter.on(",").split(dataContents));
        for (String string : set) {
            System.out.println(string);
        }
        
        if (!Strings.isNullOrEmpty(dataContents)) {
            if (dataContents.contains("profit")) {
                System.out.println("contains profit.");
            }
            // {["cr","revenue","cost","profit"]}
            Data data = new Gson().fromJson("{\"data\":[" + dataContents + "]}", Data.class);
            String[] datasStrings = data.getData();
            Set<String> set2 = Sets.newHashSet(datasStrings);
            for (String string : set2) {
                System.out.println(string);
            }
        }
        
        System.out.println("ddddddddddddddddddddddddd");
        System.out.println(Utils.xchangePreProcessIsNeed(dataStr));
        System.out.println(Utils.xchangePreProcessIsNeed(q1dataStr));
        System.out.println(Utils.xchangePreProcessIsNeed(q2dataStr));
        System.out.println(Utils.xchangePreProcessIsNeed(q3dataStr));
        System.out.println(Utils.xchangePreProcessIsNeed(q4dataStr));
        
        
        String dataCtns = Utils.getNodeXxxContents(queryData, Utils.DATA_SEG);
        
        if (Utils.xchangePreProcessIsNeed(dataCtns)) {
            List<String> list = XchangeUtils.xchangePreProcess(dataCtns);
            System.out.println(Joiner.on(",").join(list));
            System.out.println(queryData);
            System.out.println(Utils.replaceArrNodeByTag(queryData, list, Utils.DATA_SEG));
            System.out.println(Utils.replaceArrNodeByTag(queryData, new ArrayList<String>(), Utils.DATA_SEG));
            System.out.println(Utils.replaceArrNodeByTag(queryData, null, Utils.DATA_SEG));
        }else {
            System.out.println(queryData);
        }
        
        System.out.println("**************************");
        System.out.println(XchangeUtils.xchangePreProcessValid(query_param));
        System.out.println("**************************");
        ReportResult reportResult = InitTestDatas.initTestDatas();
        List<Object[]> datasList = reportResult.getData().getData();
        
        String datas = YeahmobiUtils.rmWhiteSpace(Utils.getNodeXxxContents(query_param, Utils.DATA_SEG)).replace("\"", "");
        String groups = YeahmobiUtils.rmWhiteSpace(Utils.getNodeXxxContents(query_param, Utils.DIM_SEG)).replace("\"", "");
        // String rslt_datas = Utils.getNodeXxxContents(UtilsTmp.xchangePreProcessValid(query_param), Utils.DATA_SEG);
        // String rslt_datas = YeahmobiUtils.rmWhiteSpace("aff_id,  transaction_id,  itvl_hour,  currency,  unique_click,  click,  conversion,  rows,  revenue");
        String rslt_datas = YeahmobiUtils.rmWhiteSpace(Joiner.on(",").join(datasList.get(0)));
        
        List<String> datasHeader_rslt = Lists.newArrayList(Splitter.on(",").split(rslt_datas));
        Set<String> datasHeader_qry = Sets.newHashSet(Splitter.on(",").split(Joiner.on(",").join(groups, datas)));
        SetView<String> diffs = Sets.difference(Sets.newHashSet(datasHeader_rslt), datasHeader_qry);
        
        System.out.println(datasHeader_rslt.size() + Arrays.toString(Iterables.toArray(datasHeader_rslt, String.class)));
        System.out.println(datasHeader_qry.size() + Arrays.toString(Iterables.toArray(datasHeader_qry, String.class)));
        System.out.println(diffs.size() + Arrays.toString(Iterables.toArray(diffs, String.class)));
        
        Map<String, Integer> diffMap = Maps.newHashMap();
        
        for (int i = 0; i < datasHeader_rslt.size(); i++) {
            String aggMetric = datasHeader_rslt.get(i);
            if (diffs.contains(aggMetric)) {
                diffMap.put(aggMetric, i);
            }
        }
//        for (int i = 0; i < datasHeader_rslt.size(); i++) {
//            String aggMetric = datasHeader_rslt.get(i);
//            if (diffs.contains(aggMetric)) {
//                diffMap.put(aggMetric, 8);
//            }
//        }
        
        if (diffMap.size() > 0) {
            for (Entry<String, Integer> entry: diffMap.entrySet()) {
                String key = entry.getKey();
                int index = entry.getValue();
                System.out.println(key + " - " + index);
            }
        }
        
        
        if (null != datasList && datasList.size() > 0 && diffMap.size() > 0) {
            System.out.println("/////////////////////////////" + datasList.size());
            for (Object[] objects : datasList) {
                System.out.println(Arrays.toString(objects));
                for (Entry<String, Integer> entry : diffMap.entrySet()) {
                    String key = entry.getKey();
                    int index = entry.getValue();
                    objects[index] = null;
                }
                System.out.println(Arrays.toString(objects));
                System.out.println(Arrays.toString(Joiner.on(",").skipNulls().join(objects).split(",")));
                System.out.println("------------------------");
            }
            
        }
    }
    
}

class XchangeUtils{
    public static List<String> xchangePreProcess(String dataContents) {
        List<String> result = null;
        Data data = new Gson().fromJson("{\"data\":[" + dataContents + "]}", Data.class);
        String[] datasStrings = data.getData();
        Set<String> dataSet = Sets.newHashSet(datasStrings);
        if (null != dataSet && dataSet.size() > 0) {
            String xchangeAggMetrics = "revenue,cost,profit";
            dataSet = Sets.newHashSet(Sets.union(Sets.newHashSet(Splitter.on(",").split(xchangeAggMetrics)), dataSet));
            result = Lists.newArrayList(dataSet);
            for (int i = 0; i < result.size(); i++) {
                result.set(i, wrapWithQuote(result.get(i)));
            }
        }
        return result;
    }
    
    public static List<String> xchangePreProcessor(String dataContents) {
        List<String> result = null;
        String[] datasStrings = YeahmobiUtils.rmWhiteSpace(dataContents).replace(Utils.PUNCTUATION_QUOTE, "").split(Utils.PUNCTUATION_COMMA);
        if (null != datasStrings && datasStrings.length > 0) {
            result = Lists.newArrayList(datasStrings);
            if (result.contains("profit")) {
                if (!result.contains("cost")) {
                    result.add("cost");
                }
                if (!result.contains("revenue")) {
                    result.add("revenue");
                }
            }
            
            for (int i = 0; i < result.size(); i++) {
                result.set(i, wrapWithQuote(result.get(i)));
            }
        }
        return result;
    }
    
    private static String wrapWithQuote(String str) {
        return (!Strings.isNullOrEmpty(str)) ? (Utils.PUNCTUATION_QUOTE + str + Utils.PUNCTUATION_QUOTE) : "";
    }
    
    public static String xchangePreProcessValid(String queryData) {
        String result = "";
        if (!Strings.isNullOrEmpty(queryData)) {
            String dataCtns = Utils.getNodeXxxContents(queryData, Utils.DATA_SEG);
            
            if (Utils.xchangePreProcessIsNeed(dataCtns)) {
                List<String> list = XchangeUtils.xchangePreProcessor(dataCtns);
                result = Utils.replaceArrNodeByTag(queryData, list, Utils.DATA_SEG);
            }else {
                result = queryData;
            }
        }
        return result;
    }
}

class Data{
    private String[] data;

    public Data() {
        super();
    }

    public Data(String[] data) {
        super();
        this.data = data;
    }

    public String[] getData() {
        return data;
    }

    public void setData(String[] data) {
        this.data = data;
    }
    
}

class InitTestDatas{
    public static ReportResult initTestDatas() {
        ReportResult reportResult = new ReportResult();
        
        Object[] rslt_header = Iterables.toArray(Splitter.on(",").split("aff_id, transaction_id, itvl_hour, currency, unique_click, click, conversion, rows, revenue"), String.class);
        Object[] rslt_datas1 = Iterables.toArray(Splitter.on(",").split("65536, 51b57678-09d3-4d6f-91c6-85a9a50cbb50, 6, GBP, 1, 1, 0, 1, 0.000"), String.class);
        Object[] rslt_datas2 = Iterables.toArray(Splitter.on(",").split("65537, ab5224f2-6285-4b62-86ab-50f7e95b8cc1, null, GBP, 1, 1, 0, 1, 0.000"), String.class);
        Object[] rslt_datas3 = Iterables.toArray(Splitter.on(",").split("65538, de8a2cd9-570d-467a-abb6-5b7ea7a6fad5, 8, GBP, 1, 1, 1, 2, 20.000"), String.class);
        List<Object[]> objectsList = Lists.newArrayList(rslt_header,rslt_datas1,rslt_datas2,rslt_datas3);
        
        ReportPage page = new ReportPage();
        page.setPagenumber(1);
        page.setTotal(200);
        
        Entity entity = new Entity();
        entity.setPage(page);
        entity.setData(objectsList);
        
        reportResult.setData(entity);
        reportResult.setFlag("ok");
        reportResult.setMsg("success");
        reportResult.setPage(page);
        
        return reportResult;
    }
}
