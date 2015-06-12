package com.yeahmobi.datasystem.query.process;

/**
 * Created by yangxu on 3/17/14.
 */

import com.google.common.base.Joiner;
import com.yeahmobi.datasystem.query.antlr4.DruidReportParser;
import org.apache.log4j.Logger;

public class PostProcessorFactory {

	private static Logger logger = Logger.getLogger(PostProcessorFactory.class);

	public static PostProcessor create(QueryType type, DruidReportParser parser) {
		switch (type) {
		case GROUPBY:
			return new GroupByPostProcessor(parser);
		case TIMESERIES:
			return new TimeSeriesPostProcessor(parser);
		case TOPN:
			return new TopnPostProcessor(parser);
		case IMPALAGROUP:
			return new ImpalaGroupByPostProcess(parser);
		case IMPALATIMESERIES:
			return new ImpalaTimeSeriesPostProcess(parser);
		case IMPALAGROUPFORCACHE:
			return new ImpalaGroupByPostProcessForCache(parser);
		case IMPALATIMESERIESFORCACHE:
			return new ImpalaTimeSeriesPostProcessForCache(parser);
		default:
			String msg = "unknown query type:" + type + ", support[" + Joiner.on(",").join(QueryType.values());
			logger.error(msg);
			return null;
		}
	}
}
