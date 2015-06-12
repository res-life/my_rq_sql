package com.yeahmobi.datasystem.query.pretreatment;

/**
 * 常规查询处理
 * @author dylan.zhang@yeahmobi.com
 * @date 2014年9月17日 下午2:43:20
 */
public class GeneralQueryHandler extends PretreatmentHandler {

	@Override
	public void handleRequest(ReportContext reportContext) {
			if(getNextHandler()!=null){
				getNextHandler().handleRequest(reportContext);
			}
	}

}
