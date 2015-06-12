package com.yeahmobi.datasystem.query.reportrequest;

import org.apache.log4j.Logger;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONException;

public class RouterRequestFactory {
	private static Logger logger = Logger.getLogger(RouterRequestFactory.class);

	/**
	 * string -> object
	 * 
	 * @throws JSONException
	 */
	public static RouterRequest toObject(String params) {
		RouterRequest routerRequest = null;
		try {
			routerRequest = JSON.parseObject(params, RouterRequest.class);
		} catch (JSONException e) {
			logger.error("convert RouterRequest to object failed:", e);
			throw e;
		}
		return routerRequest;
	}

	/**
	 * object -> string
	 * 
	 * @param obj
	 * @return
	 * @throws JSONException
	 */
	public static String toString(RouterRequest obj) {
		String result = "";
		try {
			result = JSON.toJSONString(obj);
		} catch (JSONException e) {
			logger.error("convert RouterRequest to string failed: ", e);
			throw e;
		}
		return result;
	}
}
