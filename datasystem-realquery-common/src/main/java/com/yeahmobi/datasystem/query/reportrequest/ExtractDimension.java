package com.yeahmobi.datasystem.query.reportrequest;

/**
 * 字段截取的对象
 * @author ellis.wang@yeahmobi.com
 * @date 2015年02月12日 下午17:43:22
 */
public class ExtractDimension {
	/*
	 * 需要使用js截取字段的维度
	 */
	private String extractDimension;
	/*
	 * 使用的js函数
	 */
	private String jsFunction;
	public String getExtractDimension() {
		return extractDimension;
	}
	public void setExtractDimension(String extractDimension) {
		this.extractDimension = extractDimension;
	}
	public String getJsFunction() {
		return jsFunction;
	}
	public void setJsFunction(String jsFunction) {
		this.jsFunction = jsFunction;
	}
	
	

}
