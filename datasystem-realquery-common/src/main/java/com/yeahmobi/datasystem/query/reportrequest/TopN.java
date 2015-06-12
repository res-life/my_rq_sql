package com.yeahmobi.datasystem.query.reportrequest;

/**
 * topn对象
 * @author dylan.zhang@yeahmobi.com
 * @date 2014年10月8日 下午2:48:22
 */
public class TopN {
	private String metricvalue;
	private int threshold;
	public String getMetricvalue() {
		return metricvalue;
	}
	public void setMetricvalue(String metricvalue) {
		this.metricvalue = metricvalue;
	}
	public int getThreshold() {
		return threshold;
	}
	public void setThreshold(int threshold) {
		this.threshold = threshold;
	} 
}
