package com.yeahmobi.datasystem.query.akka.http;

/**
 * Created by oscar.gao on 8/4/14.
 */

import java.util.List;

import com.google.common.base.Strings;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.yeahmobi.datasystem.query.akka.QueryConfig;
import com.yeahmobi.datasystem.query.akka.QueryConfigModule;
import com.yeahmobi.datasystem.query.akka.cache.CacheTool;
import com.yeahmobi.datasystem.query.akka.cache.QueryCacheFactory;
import com.yeahmobi.datasystem.query.akka.cache.XchangeRateCacheToolChest;
import com.yeahmobi.datasystem.query.antlr4.DruidReportParser;
import com.yeahmobi.datasystem.query.assist.XchangeRateCacheHelper;
import com.yeahmobi.datasystem.query.meta.ReportPage;
import com.yeahmobi.datasystem.query.meta.ReportResult;
import com.yeahmobi.datasystem.query.process.PostProcessor;
import com.yeahmobi.datasystem.query.process.PostProcessorFactory;
import com.yeahmobi.datasystem.query.process.QueryContext;
import com.yeahmobi.datasystem.query.process.XchangeProcessor;
import com.yeahmobi.datasystem.query.utils.Utils;

/**
 * post process utility
 *
 */
public class PostProcessUtil {

	public static ReportResult postProcess(Object res, QueryContext ctx,
			DruidReportParser parser, int size) {
		
		List<Object> ret = (List<Object>) res;
		PostProcessor postProcessor = PostProcessorFactory.create(
				ctx.getQueryType(), parser);
		// 后处理阶段
		// ReportResult reportResult = postProcessor.process();
		// add by martin 20140701 start
		ReportResult reportResult = null;
		String queryData = ctx.getQueryParam();
		if (needConvertCurrency(queryData)) {
			Injector injector = Guice.createInjector(new QueryConfigModule());
			QueryConfig cfg = injector.getInstance(QueryConfig.class);
			CacheTool xchangeCacheTool = new XchangeRateCacheToolChest(
					QueryCacheFactory.create(cfg.getXchangeCacheType()),
					cfg.getXchangeCacheTtlFunc());

			// reportResult = new XchangeProcessor(postProcessor.process(),
			// Utils.getCurrencyTypeFromQuery(queryData),
			// xchangeCacheTool).exchange();
			reportResult = new XchangeProcessor(postProcessor.process(ret),
					queryData, xchangeCacheTool).exchange();
		} else {
			reportResult = postProcessor.process(ret);
		}
		// add by martin 20140701 end
		ReportPage reportPage = new ReportPage();
		reportPage.setTotal(size);
		reportPage.setPagenumber(parser.page);
		reportResult.setPage(reportPage);
		return reportResult;
	}

	public static boolean needConvertCurrency(String queryData) {
		return XchangeRateCacheHelper.xchangeIsEnabled()
				&& (!Strings.isNullOrEmpty(Utils
						.getCurrencyTypeFromQuery(queryData)))
				&& XchangeRateCacheHelper.paramContainsCurrency(Utils
						.getNodeXxxContents(queryData, Utils.DIM_SEG));
	}
}
