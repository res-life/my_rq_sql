package com.yeahmobi.datasystem.query.pretreatment;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.joda.time.Period;

import com.google.common.base.Strings;
import com.yeahmobi.datasystem.query.meta.IntervalTable;
import com.yeahmobi.datasystem.query.meta.IntervalUnit;
import com.yeahmobi.datasystem.query.reportrequest.ReportParam;

/**
 * 解析 report request<br>
 * 确定请求的特点<br>
 * 是report的context<br>
 */
public class ReportContext {

	private ReportParam reportParam;

	private String style;// 查询类型

	public ReportContext(ReportParam reportParam, String style) {
		this.reportParam = reportParam;
		this.style = style;
	}

	public ReportParam getReportParam() {
		return reportParam;
	}

	/**
	 * 是否时间维度插入DB
	 * 
	 * @return
	 */
	public boolean isDoTimeDb() {
		final Period timePeriod;
		boolean isDoTimeDb = false;
		/* finalize set QueryGranularity */
		List<String> groups = reportParam.getGroup() != null ? reportParam.getGroup(): new ArrayList<String>();
		List<String> intervalUnits = new ArrayList<String>();
		for (String dimension : groups) {
			if (IntervalTable.contains(dimension)) {
				intervalUnits.add(dimension);
			}
		}
		if (!(intervalUnits == null || intervalUnits.isEmpty())) {

			if (IntervalUnit.isSingle(intervalUnits)) {
				timePeriod = IntervalUnit.singleIntervalUnit(intervalUnits);
				if (!(intervalUnits.get(0).equals("year") || intervalUnits.get(0).equals("day"))) {
					isDoTimeDb = true;
				}
			} else if (IntervalUnit.isContinuous(intervalUnits)) {
				timePeriod = IntervalUnit.continuousIntervalUnit(intervalUnits);
				if (!IntervalUnit.isSingle(timePeriod)) {
					isDoTimeDb = true;
				}
			} else {
				timePeriod = IntervalUnit.compoundIntervalUnit(intervalUnits);
				if (!IntervalUnit.isSingle(timePeriod)) {
					isDoTimeDb = true;
				}
			}
		}
		return isDoTimeDb;
	}

	/**
	 * 是否明细查询
	 * 
	 * @return
	 */
	public boolean isDetailQuery() {
		if ("file".equals(reportParam.getSettings().getReturn_format())) {
			return true;
		} else {
			return Strings.isNullOrEmpty(reportParam.getSettings().getProcess_type()) ? false : reportParam
					.getSettings().getProcess_type().contains("detail");
		}
	}

	/**
	 * 是否LP
	 * 
	 * @return
	 */
	public boolean isLp() {
		if (!Strings.isNullOrEmpty(reportParam.getSettings().getProcess_type())) {
			return reportParam.getSettings().getProcess_type().contains("lp");
		} else {
			return false;
		}
	}

	/**
	 * 是否同步
	 * 
	 * @return
	 */
	public boolean isSyncProcess() {

		// 兼容旧的接口

		String returnFormat = reportParam.getSettings().getReturn_format();
		String processType = reportParam.getSettings().getProcess_type();
		return isSyncProcess(returnFormat, processType);
	}

	public static boolean isSyncProcess(String returnFormat, String processType) {

		// 兼容旧的接口
		List<String> processTypeItems = null;
		if (null != processType) {
			processTypeItems = Arrays.asList(processType.split(","));
		}

		if ("file".equals(returnFormat)) {
			return false;
		} else {
			if (processTypeItems == null || (!processTypeItems.contains("async"))) {
				return true;
			} else {
				return false;
			}
		}
	}

	public String getFormat() {

		// 兼容旧的接口
		String returnFormat = reportParam.getSettings().getReturn_format();
		if ("file".equals(returnFormat)) {
			return "csv";
		} else if ("pretty".equals(style)) {
			return "html";
		} else if (null == returnFormat) {
			return "json";
		} else {
			return returnFormat;
		}
	}
	
	public boolean isDoImpalaByPro() {
		// 兼容旧的接口
		List<String> processTypeItems = null;
		String processType = reportParam.getSettings().getProcess_type();
		if (null != processType) {
			processTypeItems = Arrays.asList(processType.split(","));
		}

		if (processTypeItems == null || (!processTypeItems.contains("impala"))) {
			return false;
		} else {
			return true;
		}
	}
	
}
