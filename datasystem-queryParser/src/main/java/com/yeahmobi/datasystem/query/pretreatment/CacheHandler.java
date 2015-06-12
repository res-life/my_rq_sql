package com.yeahmobi.datasystem.query.pretreatment;

/**
 * 缓存判断
 * @author dylan.zhang@yeahmobi.com
 * @date 2014年9月17日 下午2:56:30
 */
public class CacheHandler extends PretreatmentHandler {

	@Override
	public void handleRequest(ReportContext reportContext) {
			if(getNextHandler()!=null){
				getNextHandler().handleRequest(reportContext);
			}

	}

}
