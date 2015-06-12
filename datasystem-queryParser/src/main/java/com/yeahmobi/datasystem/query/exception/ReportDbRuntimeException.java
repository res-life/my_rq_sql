package com.yeahmobi.datasystem.query.exception;

public class ReportDbRuntimeException extends ReportRuntimeException {

	public ReportDbRuntimeException(String formatText, Object... arguments) {
		super(formatText, arguments);
	}
	
	public ReportDbRuntimeException(Throwable cause, String formatText, Object... arguments) {
		super(cause, formatText,arguments);
	}
	
	private static final long serialVersionUID = 1L;
}
