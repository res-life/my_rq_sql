package com.yeahmobi.datasystem.query.utils;

import java.util.List;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;

public class XchangeUtils {
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