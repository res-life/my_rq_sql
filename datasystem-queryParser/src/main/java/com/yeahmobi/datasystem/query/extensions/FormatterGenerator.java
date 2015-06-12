package com.yeahmobi.datasystem.query.extensions;

import java.io.OutputStream;

import ro.fortsoft.pf4j.ExtensionPoint;

import com.yeahmobi.datasystem.query.meta.ReportResult;

/**
 * xls, csv, json, html...格式生成
 *
 */
public interface FormatterGenerator extends ExtensionPoint{

	String asString(ReportResult reportResult);
	
	void write(ReportResult reportResult, OutputStream outputStream);
	
	String getName();
}
