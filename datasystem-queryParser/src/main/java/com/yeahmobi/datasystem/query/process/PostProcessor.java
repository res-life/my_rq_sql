package com.yeahmobi.datasystem.query.process;

/**
 * Created by yangxu on 3/17/14.
 */

import java.util.List;

import com.yeahmobi.datasystem.query.antlr4.DruidReportParser;
import com.yeahmobi.datasystem.query.meta.ReportResult;

public abstract class PostProcessor {

	final DruidReportParser parser;

	public PostProcessor(DruidReportParser parser) {
		this.parser = parser;
	}

	abstract public ReportResult process(List<?> input);
}
