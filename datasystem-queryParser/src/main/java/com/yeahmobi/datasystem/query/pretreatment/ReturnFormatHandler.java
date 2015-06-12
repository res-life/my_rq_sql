package com.yeahmobi.datasystem.query.pretreatment;

/**
 * 返回值判断
 * @author dylan.zhang@yeahmobi.com
 * @date 2014年9月17日 下午2:52:59
 */
public class ReturnFormatHandler extends PretreatmentHandler {

	@Override
	public void handleRequest(ReportContext reportContext) {
		
			if(getNextHandler()!=null){
				getNextHandler().handleRequest(reportContext);
			}
		
	}

}
