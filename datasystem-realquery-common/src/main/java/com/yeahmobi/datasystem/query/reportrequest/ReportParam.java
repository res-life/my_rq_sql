package com.yeahmobi.datasystem.query.reportrequest;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.yeahmobi.datasystem.query.reportrequest.Settings.Pagination;

public class ReportParam {

	private Settings settings;
	private String currency_type;
	private List<String> group;
	private List<String> data;
	private String filters;
	private List<Map<String, Object>> sort = new ArrayList<Map<String, Object>>();
	private TopN topn;
	private List<ExtractDimension> extract;
	private transient String brokerUrl;

	public Settings getSettings() {
		return settings;
	}

	public void setSettings(Settings settings) {
		this.settings = settings;
	}

	public String getCurrency_type() {
		return currency_type;
	}

	public void setCurrency_type(String currency_type) {
		this.currency_type = currency_type;
	}

	public List<String> getGroup() {
		return group;
	}

	public void setGroup(List<String> group) {
		this.group = group;
	}

	public List<String> getData() {
		return data;
	}

	public void setData(List<String> data) {
		this.data = data;
	}

	public String getFilters() {
		return filters;
	}

	public void setFilters(String filters) {
		this.filters = filters;
	}

	public TopN getTopn() {
		return topn;
	}

	public void setTopn(TopN topn) {
		this.topn = topn;
	}


	public List<ExtractDimension> getExtract() {
		return extract;
	}

	public void setExtract(List<ExtractDimension> extract) {
		this.extract = extract;
	}

	public String getBrokerUrl() {
		return brokerUrl;
	}

	public void setBrokerUrl(String brokerUrl) {
		this.brokerUrl = brokerUrl;
	}

	public String toString() {
		return ReportParamFactory.toString(this);
	}

	public String toStringForL1() {

		String preReportId = settings.getReport_id();

		settings.setReport_id("");

		String keyForL1Cache = toString();

		settings.setReport_id(preReportId);

		return keyForL1Cache;
	}

	public String toStringForL2WithSort() {
		Pagination prePage = settings.getPagination();
		String preReportId = settings.getReport_id();

		Pagination zeorPage = new Settings.Pagination(0, 0, 0);
		settings.setPagination(zeorPage);
		settings.setReport_id("");

		String strForL2Cache = toString();

		settings.setPagination(prePage);
		settings.setReport_id(preReportId);

		return strForL2Cache;
	}

	public List<Map<String, Object>> getSort() {
		return sort;
	}

	public void setSort(List<Map<String, Object>> sort) {
		this.sort = sort;
	}

	public String toStringForL2WithoutSort() {
		Pagination prePage = settings.getPagination();
		String preReportId = settings.getReport_id();
		List<Map<String, Object>> preSort = sort;

		Pagination zeorPage = new Settings.Pagination(0, 0, 0);
		settings.setPagination(zeorPage);
		settings.setReport_id("");
		sort = null;

		String strForL2Cache = toString();

		settings.setPagination(prePage);
		settings.setReport_id(preReportId);
		sort = preSort;
		return strForL2Cache;
	}
}
