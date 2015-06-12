package com.yeahmobi.datasystem.query.utils;

import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.collect.Sets.SetView;
import com.google.gson.Gson;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.yeahmobi.datasystem.query.akka.QueryConfig;
import com.yeahmobi.datasystem.query.akka.QueryConfigModule;
import com.yeahmobi.datasystem.query.assist.XchangeRateCacheHelper;
import com.yeahmobi.datasystem.query.meta.CurrencyType;
import com.yeahmobi.datasystem.query.meta.ReportID;
import com.yeahmobi.datasystem.query.meta.ReturnFormat;

public class Utils {
    private final static Logger logger = Logger.getLogger(Utils.class);
    
    public final static String DATA_SEG = "\"data\":";
    public final static String DIM_SEG = "\"group\":";
    public final static String TOKEN_B_BRACE_L = "{";
    public final static String TOKEN_B_BRACE_R = "}";
    public final static String TOKEN_M_BRACE_L = "[";
    public final static String TOKEN_M_BRACE_R = "]";
    public final static String PUNCTUATION_COMMA = ",";
    public final static String PUNCTUATION_QUOTE = "\"";
    
    // TODO refactor 
    public static String getReportIDFromQuery(String query) {
        Gson gson = new Gson();
        String[] params = query.split(",");
        for (String string : params) {
            if (string.contains("report_id")) {
                String tmpstr = "";
                if (string.contains(TOKEN_B_BRACE_L)) {
                    return getReportIDFromQuery(string.replace(TOKEN_B_BRACE_L, PUNCTUATION_COMMA));
                }else if (string.contains(TOKEN_B_BRACE_R)) {
                    return getReportIDFromQuery(string.replace(TOKEN_B_BRACE_R, PUNCTUATION_COMMA));
                }else {
                    tmpstr = TOKEN_B_BRACE_L + string + TOKEN_B_BRACE_R; 
                }
                ReportID reportID = gson.fromJson(tmpstr, ReportID.class);
                return reportID.getReport_id();
            }
        }
        return "";
    }
    
 // TODO refactor 
    public static String getRetnFmtFromQuery(String query) {
        Gson gson = new Gson();
        String[] params = query.split(",");
        for (String string : params) {
            if (string.contains("return_format")) {
                String tmpstr = "";
                if (string.contains(TOKEN_B_BRACE_L)) {
                    return getReportIDFromQuery(string.replace(TOKEN_B_BRACE_L, PUNCTUATION_COMMA));
                }else if (string.contains(TOKEN_B_BRACE_R)) {
                    return getReportIDFromQuery(string.replace(TOKEN_B_BRACE_R, PUNCTUATION_COMMA));
                }else {
                    tmpstr = TOKEN_B_BRACE_L + string + TOKEN_B_BRACE_R; 
                }
                ReturnFormat returnFormat = gson.fromJson(tmpstr, ReturnFormat.class);
                return returnFormat.getReturn_format();
            }
        }
        return "";
    }
    
 // TODO refactor 
    public static String getCurrencyTypeFromQuery(String query) {
        Gson gson = new Gson();
        String[] params = query.split(",");
        for (String string : params) {
            if (string.contains("currency_type")) {
                String tmpstr = "";
                if (string.contains(TOKEN_B_BRACE_L)) {
                    return getCurrencyTypeFromQuery(string.replace(TOKEN_B_BRACE_L, ""));
                }else if (string.contains(TOKEN_B_BRACE_R)) {
                    return getCurrencyTypeFromQuery(string.replace(TOKEN_B_BRACE_R, ""));
                }else {
                    tmpstr = TOKEN_B_BRACE_L + string + TOKEN_B_BRACE_R; 
                }
                CurrencyType currencyType = gson.fromJson(tmpstr, CurrencyType.class);
                return currencyType.getCurrency_type();
            }
        }
        return "";
    }
    
 // TODO refactor 
    
    public static boolean containsXxxNode(String query, String tag) {
        boolean result = false;
        if (!Strings.isNullOrEmpty(query)) {
            int index = query.indexOf(tag);
            if (-1 != index) {
                result = true;
            }
        }
        return result;
    }
    
 // TODO refactor 
    public static boolean containsDataNode(String query) {
        boolean result = false;
        if (!Strings.isNullOrEmpty(query)) {
            int index = query.indexOf(DATA_SEG);
            if (-1 != index) {
                result = true;
            }
        }
        return result;
    }
    
 // TODO refactor 
    public static boolean containsGroupNode(String query) {
        boolean result = false;
        if (!Strings.isNullOrEmpty(query)) {
            int index = query.indexOf(DIM_SEG);
            if (-1 != index) {
                result = true;
            }
        }
        return result;
    }
    
    public static String addDataFieldClick(String query) {
        String result = "";
        if (!Strings.isNullOrEmpty(query)) {
            if (!containsDataNode(query)) {
                result = TOKEN_B_BRACE_L + DATA_SEG + TOKEN_M_BRACE_L + "\"click\"" + TOKEN_M_BRACE_R + PUNCTUATION_COMMA + query.substring(query.indexOf(TOKEN_B_BRACE_L) + 1, query.length());
            }else {
                result = query;
            }
        }
        return result;
    }
    
    public static String addDataFieldRows(String query) {
        String result = "";
        if (!Strings.isNullOrEmpty(query)) {
            if (!containsDataNode(query)) {
                result = TOKEN_B_BRACE_L + DATA_SEG + TOKEN_M_BRACE_L + "\"rows\"" + TOKEN_M_BRACE_R + PUNCTUATION_COMMA + query.substring(query.indexOf(TOKEN_B_BRACE_L) + 1, query.length());
            }else {
                result = query;
            }
        }
        return result;
    }
    
    public static String addGroupDimensionTRID0(String query) {
        String result = "";
        if (!Strings.isNullOrEmpty(query)) {
            String oldStr = getNodeXxx(query, DIM_SEG);
            String newStr = DIM_SEG + TOKEN_M_BRACE_L + getNodeXxxContents(query, DIM_SEG) + PUNCTUATION_COMMA +"\"transaction_id\"" + TOKEN_M_BRACE_R;
            result = (!Strings.isNullOrEmpty(oldStr)) ? query.replace(oldStr, newStr) : (TOKEN_B_BRACE_L + DIM_SEG + TOKEN_M_BRACE_L + "\"transaction_id\"" + TOKEN_M_BRACE_R + PUNCTUATION_COMMA + query.substring(query.indexOf(TOKEN_B_BRACE_L), query.length()));
        }
        return result;
    }
    
    public static String addGroupDimensionTRID(String query) {
        String result = "";
        if (!Strings.isNullOrEmpty(query)) {
            String oldStr = getNodeXxx(query, DIM_SEG);
            String newStr = DIM_SEG + TOKEN_M_BRACE_L
                    + getNodeXxxContents(query, DIM_SEG) + PUNCTUATION_COMMA
                    + "\"transaction_id\"" + TOKEN_M_BRACE_R;
            result = (!Strings.isNullOrEmpty(oldStr)) ? query.replace(oldStr, YeahmobiUtils.rmWhiteSpace(newStr)) : (TOKEN_B_BRACE_L + DIM_SEG + TOKEN_M_BRACE_L
                    + "\"transaction_id\"" + TOKEN_M_BRACE_R
                    + PUNCTUATION_COMMA + query.substring(
                    query.indexOf(TOKEN_B_BRACE_L), query.length()));
        }
        return result;
    }
    
    public static String replaceArrNodeByTag(String query, Iterable<String> nodeContents, String tag) {
        String result = "";
        if (!Strings.isNullOrEmpty(query) && !Strings.isNullOrEmpty(tag) && (null != nodeContents)) {
            String oldCtns = getNodeXxx(query, tag);
            String newCtns = YeahmobiUtils.rmWhiteSpace(tag + TOKEN_M_BRACE_L + Joiner.on(",").join((Iterable<?>) nodeContents) + TOKEN_M_BRACE_R);
            result = (!Strings.isNullOrEmpty(oldCtns)) ? query.replace(oldCtns, newCtns) : (TOKEN_B_BRACE_L + tag + TOKEN_M_BRACE_L
                    + newCtns + TOKEN_M_BRACE_R
                    + PUNCTUATION_COMMA + query.substring(
                    query.indexOf(TOKEN_B_BRACE_L), query.length()));
        }
        return result;
    }
    
    public static String getNodeXxx(String query,String tag) {
        String result = "";
        if (!Strings.isNullOrEmpty(query) && !Strings.isNullOrEmpty(tag)) {
            String contents = getNodeXxxContents(query, tag);
            if (!Strings.isNullOrEmpty(contents)) {
                result = tag + TOKEN_M_BRACE_L + contents + TOKEN_M_BRACE_R;
            }
        }else {
            result = query;
        }
        return result;
    }
    
    public static String getNodeXxxContents(String query, String tag) {
        String result = "";
        int index = query.indexOf(tag);
        if (-1 != index) {
            int s = query.indexOf(TOKEN_M_BRACE_L, index) + 1; // skip '['
            int e = query.indexOf(TOKEN_M_BRACE_R, s);
            if (-1 == e) {
                logger.error("invalid format:" + query);
                return null;
            }
            if (e != s) { // not empty
                result= query.substring(s, e);
            }
        }
        return result;
    }
    
    public static List<String> getNodeXxxContentsAsList(String query, String tag) {
        List<String> resultList = null;
        if (-1 != query.indexOf(tag)) {
            resultList = Splitter.on(PUNCTUATION_COMMA).splitToList(getNodeXxxContents(query, tag).replace("\"", ""));
        }
        return resultList;
    }
    
    public static String getResultNodeXxx(String query,String tag) {
        String result = "";
        if (!Strings.isNullOrEmpty(query) && !Strings.isNullOrEmpty(tag)) {
            String contents = getResultNodeXxxContents(query, tag);
            if (!Strings.isNullOrEmpty(contents)) {
                result = tag + TOKEN_M_BRACE_L + contents + TOKEN_M_BRACE_R;
            }
        }else {
            result = query;
        }
        return result;
    }
    
    public static String getResultNodeXxxContents(String query, String tag) {
        String result = "";
        int index = query.indexOf(tag);
        if (-1 != index) {
            int s = query.indexOf(TOKEN_M_BRACE_L + TOKEN_M_BRACE_L, index) + 1; // skip '[['
            int e = query.indexOf(TOKEN_M_BRACE_R + TOKEN_M_BRACE_R, s) + 1;
            if (-1 == e) {
                logger.error("invalid format:" + query);
                return null;
            }
            if (e != s) { // not empty
                result= query.substring(s, e);
            }
        }
        return result;
    }
    
    public static String formatResultCSVStr(String queryResult) {
        return (!Strings.isNullOrEmpty(queryResult)) ? queryResult.replace("\"", "").replace(TOKEN_M_BRACE_L, "").replace(TOKEN_M_BRACE_R + PUNCTUATION_COMMA, "\r\n").replace(TOKEN_M_BRACE_R, "") : "";
    }
    
    public static String formatResultCSVStr(String queryResult, boolean retainQuotation) {
        String tmpStr = "";
        if (Strings.isNullOrEmpty(queryResult)) {
            return tmpStr;
        }else if (!retainQuotation) {
            tmpStr = queryResult.replace("\"", "");
        }else {
            tmpStr = queryResult;
        }
        
        return (!Strings.isNullOrEmpty(tmpStr)) ? tmpStr.replace("\\\"","").replace(TOKEN_M_BRACE_L, "").replace(TOKEN_M_BRACE_R + PUNCTUATION_COMMA, "\r\n").replace(TOKEN_M_BRACE_R, "") : "";
    }
    
    public static boolean xchangePreProcessIsNeed(String dataContents) {
        boolean boo = false;
        if (!Strings.isNullOrEmpty(dataContents)) {
            boo = dataContents.contains("profit") && (!dataContents.contains("revenue") || !dataContents.contains("cost"));
        }
        return boo;
    }
    public static void main(String[] args) {
        Injector injector = Guice.createInjector(new QueryConfigModule());
        QueryConfig cfg = injector.getInstance(QueryConfig.class);
        
         String query = "{\"data\":[\"clicks\"],\"filters\":{\"$and\":{\"campaign_id\":{\"$eq\":201405}}},\"group\":[\"hour\",\"time_stamp\",\"aff_id\"],\"settings\":{\"time\":{\"start\":1399942800,\"end\":1399946399,\"timezone\":0},\"data_source\":\"contrack_druid_datasource_ds\",\"report_id\":\"report_id-100\",\"pagination\":{\"size\":10000,\"page\":0}}}";
//         String query = "{\"filters\":{\"$and\":{\"campaign_id\":{\"$eq\":201405}}},\"settings\":{\"time\":{\"start\":1399942800,\"end\":1399946399,\"timezone\":0},\"data_source\":\"contrack_druid_datasource_ds\",\"report_id\":\"report_id-100\",\"pagination\":{\"size\":10000,\"page\":0}}}";
//         String query = "{\"data\":[\"clicks\"],\"filters\":{\"$and\":{\"campaign_id\":{\"$eq\":201405}}},\"settings\":{\"time\":{\"start\":1399942800,\"end\":1399946399,\"timezone\":0},\"data_source\":\"contrack_druid_datasource_ds\",\"report_id\":\"report_id-100\",\"pagination\":{\"size\":10000,\"page\":0}}}";
//         String query = "{\"group\":[\"hour\",\"time_stamp\",\"aff_id\"],\"filters\":{\"$and\":{\"campaign_id\":{\"$eq\":201405}}},\"settings\":{\"time\":{\"start\":1399942800,\"end\":1399946399,\"timezone\":0},\"data_source\":\"contrack_druid_datasource_ds\",\"report_id\":\"report_id-100\",\"pagination\":{\"size\":10000,\"page\":0}}}";
        // String query = "{\"settings\": {\"time\": {\"start\": 1399593600,\"end\": 1400198400,\"timezone\": 0},\"data_source\": \"ymds_druid_datasource\",\"pagination\": {\"size\": 50,\"page\": 0}},\"sort\": [],\"filters\": {\"$and\": {\"aff_manager\": {\"$eq\": \"26\"}}},\"group\": [\"offer_id\",\"aff_id\",\"aff_manager\",\"time_stamp\",\"click_ip\"]}";
//        String query = "{\"settings\": {\"report_id\":\"report_id-100\",\"time\": {\"start\": 1399593600,\"end\": 1400198400,\"timezone\": 0},\"data_source\": \"ymds_druid_datasource\",\"pagination\": {\"size\": 50,\"page\": 0}},\"sort\": [],\"filters\": {\"$and\": {\"aff_manager\": {\"$eq\": \"26\"}}},\"group\": [\"offer_id\",\"aff_id\",\"aff_manager\",\"time_stamp\",\"click_ip\"]}";
//        String query = "{\"settings\":{\"report_id\":\"SDF\",\"return_format\":\"file\",\"time\":{\"start\":1401321600,\"end\":1401325200,\"timezone\":0},\"data_source\":\"ymds_druid_datasource\",\"pagination\":{\"size\":50,\"page\":0}},\"filters\":{\"$and\":{\"log_tye\":{\"$eq\":1},\"offer_id\":{\"$eq\":14044},\"click_ip\": {\"$match\":\"105.235.130.(25[0-5]%7C2[0-4]%5C%5Cd%7C1%5C%5Cd%5C%5Cd%7C[1-9]%5C%5Cd%7C[1-9])\"}}},\"group\":[\"platform_id\",\"click_ip\",\"conv_ip\",\"offer_id\",\"aff_id\"]}";
//        String result = "{\"flag\":\"success\",\"msg\":\"ok\",\"data\":{\"page\":{\"pagenumber\":0,\"total\":3},\"data\":[[\"device_id\",\"clicks\",\"convs\",\"cost\",\"income\",\"cr\",\"epc\",\"net\",\"roi\",\"ctr\"],[\"1\",1,0,1.111,0.0000,0.000000,0.000,-1.11,-1.0000,0.0000],[\"2\",79981,4866,457499.520,40951.5928,0.060839,0.512,-416547.93,-0.9105,0.0000],[\"48\",7,1,9.442,2.2220,0.142857,0.317,-7.22,-0.7647,0.0000]]}}";
//        String result = "[{\"flag\":\"success\",\"msg\":\"ok\",\"data\":{\"page\":{\"pagenumber\":0,\"total\":1000},\"data\":[[\"offer_id\",\"aff_id\",\"aff_manager\",\"time_stamp\",\"click_ip\",\"click\"],[\"14044\",\"8862\",\"26\",\"2014-05-13T16:46:55\",\"101.62.197.30\",1],[\"14044\",\"8862\",\"26\",\"2014-05-13T19:10:13\",\"115.244.82.204\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:00\",\"69.41.14.130\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:00\",\"71.100.243.216\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:00\",\"71.178.98.215\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:00\",\"71.75.51.141\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:01\",\"192.136.227.52\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:01\",\"217.29.20.26\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:02\",\"130.166.142.208\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:03\",\"66.233.45.40\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:03\",\"99.112.62.155\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:04\",\"70.208.197.43\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:04\",\"74.36.109.6\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:05\",\"107.178.47.197\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:05\",\"24.176.16.26\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:05\",\"68.11.183.124\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:05\",\"68.110.111.240\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:06\",\"148.177.1.216\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:06\",\"150.70.172.104\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:06\",\"172.56.5.60\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:06\",\"173.27.70.5\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:06\",\"174.101.170.184\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:06\",\"184.170.87.44\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:06\",\"68.75.225.17\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:06\",\"8.28.16.254\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:06\",\"98.213.198.206\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:06\",\"99.37.172.15\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:07\",\"199.192.85.194\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:07\",\"50.159.135.12\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:08\",\"71.195.59.252\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:08\",\"8.28.16.254\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:09\",\"71.93.211.201\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:09\",\"99.8.70.189\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:10\",\"128.62.58.17\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:10\",\"173.188.55.33\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:10\",\"65.128.168.97\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:11\",\"217.29.20.26\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:11\",\"50.244.118.89\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:11\",\"73.178.230.83\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:11\",\"99.8.164.136\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:12\",\"150.70.173.56\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:12\",\"68.120.90.248\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:12\",\"75.95.78.6\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:13\",\"107.144.89.249\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:13\",\"173.66.105.249\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:13\",\"216.252.20.165\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:13\",\"50.8.127.177\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:13\",\"67.87.18.89\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:13\",\"70.197.73.127\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:13\",\"74.139.106.215\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:13\",\"74.72.180.43\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:14\",\"172.56.6.180\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:14\",\"24.214.248.111\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:14\",\"69.138.162.71\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:14\",\"72.46.154.221\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:14\",\"97.82.180.115\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:15\",\"104.3.240.53\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:15\",\"23.113.39.48\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:15\",\"68.201.34.236\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:15\",\"72.200.84.3\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:16\",\"24.230.111.242\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:17\",\"150.70.173.55\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:17\",\"50.117.34.232\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:17\",\"72.178.22.74\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:17\",\"98.249.114.37\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:18\",\"143.105.164.100\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:18\",\"71.217.30.60\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:18\",\"74.112.52.132\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:19\",\"68.2.45.94\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:19\",\"71.203.34.189\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:20\",\"184.76.46.103\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:21\",\"108.202.253.229\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:21\",\"108.71.88.144\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:21\",\"192.154.63.69\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:21\",\"67.161.150.61\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:21\",\"71.165.182.69\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:21\",\"74.197.186.135\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:21\",\"75.147.20.190\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:21\",\"99.7.37.220\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:22\",\"107.0.69.130\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:22\",\"68.50.178.107\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:22\",\"96.238.8.234\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:22\",\"99.9.56.129\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:23\",\"166.147.88.32\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:23\",\"24.171.58.131\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:23\",\"64.109.160.70\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:23\",\"74.68.99.104\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:23\",\"76.24.179.114\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:24\",\"108.51.115.127\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:24\",\"184.155.184.98\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:24\",\"74.89.158.201\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:25\",\"150.70.173.54\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:25\",\"173.217.142.112\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:25\",\"23.117.113.100\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:25\",\"67.183.143.19\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:25\",\"68.84.116.190\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:25\",\"69.94.24.205\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:28\",\"107.22.47.61\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:28\",\"129.89.51.156\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:28\",\"174.54.75.254\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:28\",\"199.17.196.1\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:28\",\"216.178.108.232\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:28\",\"24.99.126.250\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:28\",\"72.191.181.89\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:30\",\"173.18.87.99\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:30\",\"174.21.167.23\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:30\",\"67.166.245.247\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:30\",\"99.19.17.21\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:31\",\"71.246.235.139\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:31\",\"76.174.54.156\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:32\",\"137.139.40.233\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:32\",\"67.243.171.251\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:32\",\"68.60.179.86\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:33\",\"108.221.170.185\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:33\",\"66.68.37.97\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:33\",\"72.193.99.87\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:33\",\"76.182.244.230\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:34\",\"162.210.130.3\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:34\",\"172.9.30.2\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:34\",\"67.197.240.138\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:34\",\"68.10.224.136\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:34\",\"71.234.108.173\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:34\",\"97.89.240.61\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:35\",\"108.231.255.1\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:35\",\"162.192.192.185\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:35\",\"198.188.134.45\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:36\",\"104.11.136.18\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:36\",\"216.172.138.89\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:36\",\"71.200.61.10\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:36\",\"72.179.31.175\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:36\",\"99.194.48.183\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:37\",\"162.197.60.14\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:37\",\"66.66.187.196\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:37\",\"72.239.103.18\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:37\",\"76.102.255.24\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:38\",\"128.4.139.2\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:38\",\"23.113.238.46\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:38\",\"71.178.91.34\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:38\",\"71.63.44.251\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:38\",\"96.255.149.62\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:39\",\"66.74.36.29\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:39\",\"74.89.201.156\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:40\",\"204.52.215.7\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:40\",\"68.53.136.168\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:41\",\"108.168.216.104\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:41\",\"216.169.128.25\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:41\",\"23.236.125.31\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:41\",\"24.17.30.30\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:41\",\"50.182.242.75\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:41\",\"98.199.100.109\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:42\",\"162.194.120.119\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:42\",\"162.217.32.230\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:42\",\"50.128.210.147\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:42\",\"98.235.226.109\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:43\",\"108.251.77.176\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:43\",\"50.150.24.99\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:43\",\"66.190.98.31\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:44\",\"173.48.220.26\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:44\",\"71.89.34.157\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:45\",\"108.224.96.160\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:45\",\"173.29.176.32\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:45\",\"50.152.143.247\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:45\",\"96.246.56.59\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:46\",\"108.244.97.47\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:46\",\"172.56.22.233\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:46\",\"72.88.186.23\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:46\",\"97.104.28.224\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:47\",\"150.70.97.119\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:47\",\"50.180.77.154\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:47\",\"75.23.198.12\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:47\",\"98.177.163.154\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:48\",\"138.72.137.238\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:48\",\"184.153.147.189\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:48\",\"23.126.177.70\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:48\",\"24.164.69.65\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:48\",\"24.62.71.152\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:48\",\"50.161.235.206\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:48\",\"98.217.28.89\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:49\",\"108.231.88.170\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:49\",\"198.101.34.10\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:49\",\"98.109.81.152\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:50\",\"150.70.97.126\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:50\",\"67.85.182.162\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:50\",\"75.148.170.110\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:50\",\"76.170.12.115\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:50\",\"99.63.177.27\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:51\",\"139.147.111.124\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:51\",\"150.108.238.44\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:52\",\"108.236.196.187\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:52\",\"162.220.125.135\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:52\",\"216.54.159.202\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:52\",\"24.24.212.62\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:53\",\"162.217.32.230\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:53\",\"24.205.23.134\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:54\",\"172.14.46.109\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:54\",\"24.47.132.21\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:54\",\"70.109.166.62\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:54\",\"72.201.215.176\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:54\",\"76.115.73.173\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:54\",\"8.28.16.254\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:55\",\"70.215.193.93\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:55\",\"72.48.23.114\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:55\",\"96.246.0.238\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:56\",\"72.69.165.36\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:57\",\"50.175.146.4\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:57\",\"65.31.218.54\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:57\",\"8.28.16.254\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:58\",\"173.54.200.59\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:58\",\"173.71.128.64\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:58\",\"184.98.172.180\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:58\",\"198.185.18.207\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:58\",\"71.89.89.178\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:59\",\"12.231.36.2\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:59\",\"162.196.45.229\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:00:59\",\"204.107.82.174\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:01:00\",\"107.131.104.216\",1],[\"14704\",\"14736\",\"26\",\"2014-05-09T00:01:00\",\"199.17.206.1\",1]]}}]";
        String result = "[{\"flag\":\"success\",\"msg\":\"ok\",\"data\":{\"page\":{\"pagenumber\":0,\"total\":2},\"data\":[[\"day\",\"adv_sub2\",\"adv_sub3\",\"adv_sub4\",\"adv_sub5\",\"adv_sub6\",\"adv_sub7\",\"adv_sub8\",\"ref_track_site\",\"device_brand\",\"device_model\",\"platform_id\",\"device_os\",\"user_agent\",\"click_time\",\"conv_time\",\"time_diff\",\"aff_id\",\"aff_manager\",\"aff_sub1\",\"aff_sub2\",\"aff_sub3\",\"aff_sub4\",\"aff_sub5\",\"aff_sub6\",\"aff_sub7\",\"aff_sub8\",\"adv_id\",\"adv_manager\",\"adv_sub1\",\"offer_id\",\"transaction_id\",\"ref_track\",\"browser\",\"country\",\"conv_ip\",\"click_ip\",\"click\",\"unique_click\",\"cost\",\"revenue\",\"conversion\"],[\"2014-06-07\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"172.30.10.146:8080\",\"2\",\"158\",\"2\",0,\"\\\"Mozilla/5.0 (Linux; Android 4.2.1; Galaxy Nexus Build/JOP40D) AppleWebKit/535.19 (KHTML, like Gecko) Chrome/18.0.1025.166 Mobile Safari/535.19\\\"\",\"2014-06-07 02:56:36\",\"\",null,\"90010407\",\"90010453\",\"lmx-af01\",\"lmx-af02\",\"lmx-af03\",\"lmx-af04\",\"lmx-af05\",\"lmx-af06\",\"lmx-af07\",\"lmx-af08\",\"90011401\",\"90010454\",\"\",\"90010401\",\"41598993-841a-47dc-b6b9-427ec1ab7034\",\"http://172.30.10.146:8080/trace?offer_id\u003d90010401\u0026aff_id\u003d90010407\",\"5\",\"US\",\"\",\"66.87.98.137\",1,1,0.000,0.000,0],[\"2014-06-07\",\"lmx-ad02\",\"lmx-ad03\",\"lmx-ad04\",\"lmx-ad05\",\"lmx-ad06\",\"lmx-ad07\",\"lmx-ad08\",\"172.30.10.146:8080\",\"2\",\"158\",\"2\",0,\"\\\"Mozilla/5.0 (Linux; Android 4.2.1; Galaxy Nexus Build/JOP40D) AppleWebKit/535.19 (KHTML, like Gecko) Chrome/18.0.1025.166 Mobile Safari/535.19\\\"\",\"2014-06-07 02:56:36\",\"2014-06-07 02:57:51\",\"1m15s\",\"90010407\",\"90010453\",\"lmx-af01\",\"lmx-af02\",\"lmx-af03\",\"lmx-af04\",\"lmx-af05\",\"lmx-af06\",\"lmx-af07\",\"lmx-af08\",\"90011401\",\"90010454\",\"lmx-ad01\",\"90010401\",\"41598993-841a-47dc-b6b9-427ec1ab7034\",\"http://172.30.10.146:8080/trace?offer_id\u003d90010401\u0026aff_id\u003d90010407\",\"5\",\"US\",\"65.61.50.4\",\"66.87.98.137\",0,0,0.100,1.000,1]]}}]";
        String str2 = "{\"settings\":{\"report_id\":\"report_id-x007\",\"return_format\":\"file\",\"time\":{\"start\":1399420800,\"end\":1399424400, \"timezone\":0},\"data_source\":\"ymds_druid_datasource\",\"pagination\":{\"size\":50,\"page\":0}},\"filters\":{\"$and\":{\"log_tye\":{\"$eq\":1}}},\"group\":[\"aff_id \"]}";
        String currency_type_params = "{\"settings\":{\"report_id\":\"af3a0e3ea19a76157e5cd456b047d2c2\",\"return_format\":\"json\",\"data_source\":\"ymds_druid_datasource\",\"time\":{\"start\":1401811200,\"end\":1401983999,\"timezone\":0},\"pagination\":{\"size\":10000,\"page\":0}},\"group\":[\"click_time\",\"conv_time\",\"time_diff\",\"aff_id\",\"aff_sub1\",\"offer_id\",\"transaction_id\",\"ref_track\",\"country\",\"conv_ip\",\"click_ip\"],\"data\":[\"unique_click\",\"conversion\"],\"filters\":{\"$and\":{\"aff_manager\":{\"$in\":[\"90010453\"]}}},\"currency_type\":\"USD\"}";
        String currency_type_params2 = "{\"settings\":{\"report_id\":\"af3a0e3ea19a76157e5cd456b047d2c2\",\"return_format\":\"json\",\"data_source\":\"ymds_druid_datasource\",\"time\":{\"start\":1401811200,\"end\":1401983999,\"timezone\":0},\"pagination\":{\"size\":10000,\"page\":0}},\"group\":[\"click_time\",\"conv_time\",\"time_diff\",\"aff_id\",\"aff_sub1\",\"offer_id\",\"transaction_id\",\"ref_track\",\"country\",\"conv_ip\",\"click_ip\"],\"data\":[\"unique_click\",\"conversion\"],\"currency_type\":\"USD\",\"filters\":{\"$and\":{\"aff_manager\":{\"$in\":[\"90010453\"]}}}}";
        String currency_type_params3 = "{\"currency_type\":\"USD\",\"settings\":{\"report_id\":\"af3a0e3ea19a76157e5cd456b047d2c2\",\"return_format\":\"json\",\"data_source\":\"ymds_druid_datasource\",\"time\":{\"start\":1401811200,\"end\":1401983999,\"timezone\":0},\"pagination\":{\"size\":10000,\"page\":0}},\"group\":[\"click_time\",\"conv_time\",\"time_diff\",\"aff_id\",\"aff_sub1\",\"offer_id\",\"transaction_id\",\"ref_track\",\"country\",\"conv_ip\",\"click_ip\"],\"data\":[\"unique_click\",\"conversion\",\"revenue\"],\"filters\":{\"$and\":{\"aff_manager\":{\"$in\":[\"90010453\"]}}}}";
        String currency_type_params4 = "{\"settings\":{\"report_id\":\"af3a0e3ea19a76157e5cd456b047d2c2\",\"return_format\":\"json\",\"data_source\":\"ymds_druid_datasource\",\"time\":{\"start\":1401811200,\"end\":1401983999,\"timezone\":0},\"pagination\":{\"size\":10000,\"page\":0}},\"group\":[\"click_time\",\"conv_time\",\"time_diff\",\"aff_id\",\"aff_sub1\",\"offer_id\",\"transaction_id\",\"ref_track\",\"country\",\"conv_ip\",\"click_ip\"],\"data\":[\"unique_click\",\"conversion\"],\"filters\":{\"$and\":{\"aff_manager\":{\"$in\":[\"90010453\"]}}}}";
        // getNodeXxx(query, DIM_SEG);
//        System.out.println(getNodeXxxContents(query, DIM_SEG));
//        System.out.println(query);
        System.out.println(addDataFieldClick(str2));
        System.out.println(addGroupDimensionTRID(addDataFieldClick(str2)));
//        System.out.println(addGroupDimensionTRID(query));
//
//        System.out.println("---------------");
//        System.out.println(csvHeaderBuilder(query));
        System.out.println(getReportIDFromQuery(str2));
        System.out.println(getResultNodeXxx(result, DATA_SEG));
        // System.out.println(getResultNodeXxxContents(result, DATA_SEG).replace("\"", "").replace("[", "").replace("],", "\r\n").replace("]", ""));
        System.out.println("********************");
        System.out.println(formatResultCSVStr(getResultNodeXxxContents(result, DATA_SEG), true));
        System.out.println("********************");
        System.out.println(getNodeXxx(query, "data"));
        System.out.println(getNodeXxx(query, "group"));
        System.out.println(getNodeXxx(query, "filter"));
        System.out.println("currency_type = " + getCurrencyTypeFromQuery(currency_type_params4));
        String datas = getNodeXxxContents(currency_type_params3, DATA_SEG);
        System.out.println(datas);
        List<String> dataList = Splitter.on(PUNCTUATION_COMMA).splitToList(datas.replace("\"", ""));
        List<String> dataList2 = getNodeXxxContentsAsList(currency_type_params3, DATA_SEG);
        for (String string : dataList2) {
            System.out.println("str = " + string);
        }
        Set<String> aggMetrics = Sets.newHashSet(cfg.getXchangeableAggMetrics().split(PUNCTUATION_COMMA));
        Set<String> paramsData = Sets.newHashSet(dataList2);
        SetView<String> ddd = Sets.intersection(aggMetrics, paramsData);
        if (null != ddd && ddd.size() > 0) {
            System.out.println("Has agg metrics.");
            for (String string : ddd) {
                System.out.print(string);
            }
        }else {
            System.out.println("Has no agg metrics.");
        }
        
        System.out.println();
        System.out.println(XchangeRateCacheHelper.hasXchangeMetric(dataList2));
        System.out.println(new Gson().toJson(new String("AppleWebKit/535.19 (KHTML, like Gecko) "
                + "Chrome/18.0.1025.166 Mobile Safari/535.19")));
        
    }
    
    private static String csvHeaderBuilder(String query) {
        StringBuilder stringBuilder = new StringBuilder();
        
        if (!Strings.isNullOrEmpty(query)) {
            if (Utils.containsGroupNode(query) && Utils.containsDataNode(query)) {
                stringBuilder.append(Utils.getNodeXxxContents(query, Utils.DIM_SEG))
                            .append(Utils.PUNCTUATION_COMMA)
                            .append(Utils.getNodeXxxContents(query, Utils.DATA_SEG)); // "hour","time_stamp","aff_id"
            }else if (Utils.containsGroupNode(query)) {
                stringBuilder.append(Utils.getNodeXxxContents(query, Utils.DIM_SEG));
            }else if (Utils.containsDataNode(query)) {
                stringBuilder.append(Utils.getNodeXxxContents(query, Utils.DATA_SEG));
            }
        }
        
        return stringBuilder.toString();
    }
}
