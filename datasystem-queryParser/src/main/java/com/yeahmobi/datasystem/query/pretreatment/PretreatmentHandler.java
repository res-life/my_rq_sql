package com.yeahmobi.datasystem.query.pretreatment;


/**
 * 预处理责任链
 * @author dylan.zhang@yeahmobi.com
 * @date 2014年9月15日 下午5:47:58
 */
public abstract class PretreatmentHandler {
	/**
	 * 下一个处理请求对象
	 */
	protected PretreatmentHandler nextHandler=null;
	
	/**
	 * 处理请求
	 * @param reportContext
	 * @return
	 */
	public abstract void handleRequest(ReportContext reportContext);

	public PretreatmentHandler getNextHandler() {
		return nextHandler;
	}

	public void setNextHandler(PretreatmentHandler nextHandler) {
		this.nextHandler = nextHandler;
	}
}
