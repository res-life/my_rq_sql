package com.yeahmobi.datasystem.query.jersey;

import com.yeahmobi.datasystem.query.StatementType;


public class ReportServiceRequest {

	private String param;
	private String style;
	private StatementType queryType;
	
	public ReportServiceRequest(String param, String style, StatementType queryType){
		this.param = param;
		this.style = style;
		this.queryType = queryType;
	}
	
	public String getParam() {
		return param;
	}
	public void setParam(String param) {
		this.param = param;
	}
	public String getStyle() {
		return style;
	}
	public void setStyle(String style) {
		this.style = style;
	}

	public StatementType getQueryType() {
		return queryType;
	}

	public void setQueryType(StatementType queryType) {
		this.queryType = queryType;
	}
}
