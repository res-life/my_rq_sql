package com.yeahmobi.datasystem.query.extensions;

import java.util.List;

import ro.fortsoft.pf4j.ExtensionPoint;

import com.yeahmobi.datasystem.query.antlr4.DruidReportParser;
import com.yeahmobi.datasystem.query.meta.ImpalaCfgItem;
import com.yeahmobi.datasystem.query.process.QueryType;

/**
 * 
 * 
 */
public interface Impala extends ExtensionPoint {

	List<Object> doImpalaHandle(String dataSource, DruidReportParser parser, QueryType queryType);
}
