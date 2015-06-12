package com.yeahmobi.datasystem.query.exception;

public class ReportRuntimeException extends RuntimeException {
	
	public ReportRuntimeException(String formatText, Object... arguments) {
		super(String.format(formatText, arguments));
	}

	public ReportRuntimeException(Throwable cause, String formatText, Object... arguments) {
		super(String.format(formatText, arguments), cause);
	}

	private static final long serialVersionUID = 1L;
}
