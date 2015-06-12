package com.yeahmobi.datasystem.query.reportrequest;

import java.util.List;

public class RouterRequest {
	private String dataSource; 
	private List<String> groupbys;
	private List<String> aggregators;
	private List<String> filters;
	private List<String> havings;
	private Long start;
	private Long end;
	private String reportId;
	private String processType;
	public String getProcessType() {
		return processType;
	}

	public void setProcessType(String processType) {
		this.processType = processType;
	}


	public String getDataSource() {
		return dataSource;
	}

	
	public String getReportId() {
		return reportId;
	}


	public void setReportId(String reportId) {
		this.reportId = reportId;
	}


	public void setDataSource(String dataSource) {
		this.dataSource = dataSource;
	}

	public Long getStart() {
		return start;
	}

	public void setStart(Long start) {
		this.start = start;
	}

	public Long getEnd() {
		return end;
	}

	public void setEnd(Long end) {
		this.end = end;
	}

	public List<String> getGroupbys() {
		return groupbys;
	}

	public void setGroupbys(List<String> groupbys) {
		this.groupbys = groupbys;
	}

	public List<String> getAggregators() {
		return aggregators;
	}

	public void setAggregators(List<String> aggregators) {
		this.aggregators = aggregators;
	}

	public List<String> getFilters() {
		return filters;
	}

	public void setFilters(List<String> filters) {
		this.filters = filters;
	}

	public List<String> getHavings() {
		return havings;
	}

	public void setHavings(List<String> havings) {
		this.havings = havings;
	}
}
