package com.yeahmobi.datasystem.query.pretreatment;

import com.yeahmobi.datasystem.query.reportrequest.ReportParam;

/**
 * 时间维度判断处理
 * @author dylan.zhang@yeahmobi.com
 * @date 2014年9月17日 下午2:46:28
 */
public class TimeDimentionHandler extends PretreatmentHandler {

	@Override
	public void handleRequest(ReportContext reportContext) {
		// 时间维度处理
		if (reportContext.isDoTimeDb()) {
			timeDimentionAgg(reportContext);
		} else {
			// 运行处理后续的责任链上的handler
			if (getNextHandler() != null) {
				getNextHandler().handleRequest(reportContext);
			}
		}
	}

	/**
	 * TODO haiwei
	 * 
	 * @param dataSet
	 */
	private ReportParam timeDimentionAgg(ReportContext reportContext) {
		return null;
	}

}
