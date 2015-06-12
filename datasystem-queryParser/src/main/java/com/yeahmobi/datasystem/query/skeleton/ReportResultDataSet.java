package com.yeahmobi.datasystem.query.skeleton;

import java.util.Map;

import com.yeahmobi.datasystem.query.jersey.ReportServiceResult;
import com.yeahmobi.datasystem.query.meta.MsgType;
import com.yeahmobi.datasystem.query.meta.ReportResult;

/**
 * 包装ReportResult成DataSet
 *
 */
public class ReportResultDataSet extends AbstractDataSet {

	private ReportResult reportResult;
	private Map<String, String> info;

	public ReportResultDataSet(ReportResult reportResult){
		this.reportResult = reportResult;
	}
	
	@Override
	public Map<String, String> getInfo() {
		return info;
	}
	@Override
	public ReportResult getReportResult() {
		return reportResult;
	}

	@Override
	public ReportServiceResult getReportServiceResult() {
		return new ReportServiceResult(MsgType.success,reportResult);
	}

	public void setInfo(Map<String, String> additionalInfo) {
		this.info = additionalInfo;
	}
	
}
