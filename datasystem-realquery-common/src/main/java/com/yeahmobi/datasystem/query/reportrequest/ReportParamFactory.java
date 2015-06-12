package com.yeahmobi.datasystem.query.reportrequest;



import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.List;

import org.apache.log4j.Logger;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONException;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;


public class ReportParamFactory {
	private static Logger logger = Logger.getLogger(ReportParamFactory.class);
	
	/**
	 * string -> object
	 * @throws JSONException
	 */
	public static ReportParam toObject(String params) {
		ReportParam reportParam=null;
		try {
		    String filters=getFilters(params);
			reportParam=JSON.parseObject(params, ReportParam.class);
			reportParam.setFilters(filters);   
		} catch (JSONException e) {
			logger.error("convert reportparam to object failed:", e);
			throw e;
		} 
		return reportParam;
	}

	/**
	 * object -> string
	 * 特殊处理data 和 filter ,把group置于settings之后是要先解析出时区的信息.
	 * @param obj
	 * @return
	 * @throws JSONException
	 */
	public static String toString(ReportParam reportParam) {
		String result = "";
		try {
			String filtersStr = reportParam.getFilters();
			List<String> data = reportParam.getData();
			List<String> groups = reportParam.getGroup();
			
			reportParam.setFilters(null);
			reportParam.setData(null);
			reportParam.setGroup(null);
			
			result=JSON.toJSONString(reportParam);
			
			int lastIndex = result.lastIndexOf('}');
			String dataStr = "[" + Joiner.on(',').join(data) + "]";
			String groupStr = "[]";
			if(groups != null){
				groupStr = JSON.toJSONString(groups);
			}
			
			result = result.substring(0, lastIndex) + ",\"group\":" + groupStr + ",\"filters\":" + filtersStr + ",\"data\":" + dataStr + "}";
			reportParam.setFilters(filtersStr);
			reportParam.setData(data);
			reportParam.setGroup(groups);
		} catch (JSONException e) {
			logger.error("convert reportparam to string failed: ", e);
			throw e;
		}
		return result;
	}
	
	/**
	 * 字符串截取filters
	 * @param params
	 * @return
	 */
	private final static String FILTERS_SEG = "\"filters\":";
	private static String getFilters(String params){
	
		String filters="";
		int findex=params.indexOf(FILTERS_SEG);
		if(findex==-1){
			return filters;
		}
		int objectBeginIndex = 0;
		int objectEndIndex = 0;
		int beginIndex = findex + 10;
		
		JsonFactory jsonFactory = new JsonFactory();
		JsonParser jsonParser;
		String str = params.substring(beginIndex);
		Reader reader = new StringReader(str);
		try {
			jsonParser = jsonFactory.createParser(reader);
			JsonToken aToken = jsonParser.nextToken();
			if(aToken == JsonToken.START_OBJECT){
				objectBeginIndex = (int) jsonParser.getCurrentLocation().getCharOffset();
				jsonParser.skipChildren();
				objectEndIndex = (int) jsonParser.getCurrentLocation().getCharOffset();
			}else{
				throw new RuntimeException("get filters content failed");
			}
		} catch (JsonParseException e) {
			logger.error("get filters content failed: ", e);
			throw new RuntimeException("get filters content failed",e);
		} catch (IOException e) {
			logger.error("get filters content failed: ", e);
			throw new RuntimeException("get filters content failed",e);
		}
		
		filters = str.substring(objectBeginIndex, objectEndIndex+1);
		return filters;
	}
	/*private static String getFilters(String params){
		
		String filters="";
		int findex=params.indexOf(FILTERS_SEG);
		if(findex==-1){
			return filters;
		}
		int objectBeginIndex;
		int objectEndIndex;
		int sum = 0;
		int beginIndex = findex;
		
		do{
			objectBeginIndex = params.indexOf("{",beginIndex);
			objectEndIndex = params.indexOf("}",beginIndex);
			if(objectBeginIndex > objectEndIndex || objectBeginIndex == -1){
				sum = sum-1;
				beginIndex = objectEndIndex;
			}else{
				sum = sum+1;
				beginIndex = objectBeginIndex;
			}
			++beginIndex;
		}while(sum > 0);
		
		filters=params.substring(findex+10, objectEndIndex+1);
		return filters;
	}*/
	
}
