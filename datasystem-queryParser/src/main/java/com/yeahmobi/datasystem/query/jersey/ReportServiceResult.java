package com.yeahmobi.datasystem.query.jersey;

import com.yeahmobi.datasystem.query.meta.MsgType;


public class ReportServiceResult {

	private MsgType msgType;
	private Object result;
	
	public ReportServiceResult(MsgType msgType, Object result){
		this.msgType = msgType;
		this.result = result;
	}

	public MsgType getMsgType() {
		return msgType;
	}

	public void setMsgType(MsgType msgType) {
		this.msgType = msgType;
	}

	public Object getResult() {
		return result;
	}

	public void setResult(Object result) {
		this.result = result;
	}
}
