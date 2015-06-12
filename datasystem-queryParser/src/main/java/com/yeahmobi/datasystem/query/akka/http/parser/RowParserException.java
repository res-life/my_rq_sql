package com.yeahmobi.datasystem.query.akka.http.parser;

import com.yeahmobi.datasystem.query.exception.ReportRuntimeException;

public class RowParserException extends ReportRuntimeException {

	public RowParserException(String msg) {
		super(msg);
	}
	
	public RowParserException(String msg, Throwable e) {
		super(msg, e);
	}

	private static final long serialVersionUID = 1L;
}
