package com.yeahmobi.datasystem.query.reportrequest;


public class RouterResult {

	private String brokerUrl;
	private String dataSource;
	private Platform platform;
	
	public RouterResult(){
	}
	
	public RouterResult(String brokerUrl, String dataSource, Platform platform){
		this.brokerUrl = brokerUrl;
		this.dataSource = dataSource;
		this.platform = platform;
	}
	
	public String getBrokerUrl(){
		return brokerUrl;
	}
	
	public String getDataSource(){
	    return dataSource;
	}
	
	public void setBrokerUrl(String brokerUrl){
		this.brokerUrl = brokerUrl;
	}
	
	public void setDataSource(String dataSource){
		this.dataSource = dataSource;
	}

	public Platform getPlatform() {
		return platform;
	}

	public void setPlatform(Platform platform) {
		this.platform = platform;
	}
	
	
}