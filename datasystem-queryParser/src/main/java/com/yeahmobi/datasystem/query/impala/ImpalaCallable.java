package com.yeahmobi.datasystem.query.impala;

import java.util.List;
import java.util.concurrent.Callable;

import org.rmrodrigues.pf4j.web.PluginManagerHolder;

import com.yeahmobi.datasystem.query.antlr4.DruidReportParser;
import com.yeahmobi.datasystem.query.extensions.Impala;
import com.yeahmobi.datasystem.query.meta.ReportResult;
import com.yeahmobi.datasystem.query.process.PostProcessor;
import com.yeahmobi.datasystem.query.process.PostProcessorFactory;
import com.yeahmobi.datasystem.query.process.QueryType;

public class ImpalaCallable implements Callable<ReportResult> {

	private DruidReportParser parser;
	private String dataSource;
	private List<Object> res;

	public List<Object> getRes() {
		return res;
	}

	public void setRes(List<Object> res) {
		this.res = res;
	}

	public ImpalaCallable(String dataSource, DruidReportParser parser) {
		this.dataSource = dataSource;
		this.parser = parser;
	}

	@Override
	public ReportResult call() throws Exception {
		final Impala impalaPlugin = PluginManagerHolder.getPluginManager().getExtensions(Impala.class).get(0);

		final QueryType queryType = getqueryType(parser);

		// 调用plugin. 如果process_type 有impala, 则使用。 或者调用插件， 条件符合
		res = impalaPlugin.doImpalaHandle(dataSource, parser, queryType);
		// 调用后处理
		QueryType type = null;
		if (queryType.compareTo(QueryType.TIMESERIES) == 0)
			type = QueryType.IMPALATIMESERIES;
		if (queryType.compareTo(QueryType.GROUPBY) == 0)
			type = QueryType.IMPALAGROUP;
		PostProcessor postProcessor = PostProcessorFactory.create(type, parser);
		ReportResult ret = postProcessor.process(res);
		return ret;
	}

	public ReportResult getReportResultForCache() {
		final QueryType queryType = getqueryType(parser);
		QueryType type = null;
		if (queryType.compareTo(QueryType.TIMESERIES) == 0)
			type = QueryType.IMPALATIMESERIESFORCACHE;
		if (queryType.compareTo(QueryType.GROUPBY) == 0)
			type = QueryType.IMPALAGROUPFORCACHE;
		PostProcessor postProcessor = PostProcessorFactory.create(type, parser);
		ReportResult ret = postProcessor.process(getRes());
		return ret;
	}

	private QueryType getqueryType(DruidReportParser parser) {
		if (parser.groupByDimensions.isEmpty() && parser.threshold == -1) {
			return QueryType.TIMESERIES;
		} else if (parser.threshold != -1) {
			return QueryType.TOPN;
		} else {
			return QueryType.GROUPBY;
		}
	}
}
