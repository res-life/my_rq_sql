package com.yeahmobi.datasystem.query.parse;

import io.druid.jackson.DefaultObjectMapper;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Map;

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import com.yeahmobi.datasystem.query.antlr4.DruidReportParser;
import com.yeahmobi.datasystem.query.antlr4.WarpInterpreter;
import com.yeahmobi.datasystem.query.exception.FileErrorListener;
import com.yeahmobi.datasystem.query.extensions.DataSourceView;
import com.yeahmobi.datasystem.query.extensions.Dimentions;
import com.yeahmobi.datasystem.query.extensions.Metrics;
import com.yeahmobi.datasystem.query.meta.DimensionTable;
import com.yeahmobi.datasystem.query.meta.MetricAggTable;
import com.yeahmobi.datasystem.query.pretreatment.Pretreatment;
import com.yeahmobi.datasystem.query.process.QueryContext;
import com.yeahmobi.datasystem.query.process.QueryFactory;
import com.yeahmobi.datasystem.query.reportrequest.ReportParam;
import com.yeahmobi.datasystem.query.reportrequest.ReportParamFactory;
import com.yeahmobi.datasystem.query.skeleton.DataSourceViews;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ DataSourceViews.class })
public class Testantlr {

	@Before
	public void before() {
		PowerMockito.mockStatic(DataSourceViews.class);
	}

	@Test
	public void test1() throws Exception {
//		String queryParams0 = "{\"data\":[\"clicks\"],\"filters\":{\"$and\":{\"campaign_id\":{\"$match\":\"^d_\"}}},\"group\":[\"campaign_id\"],\"settings\":{\"time\":{\"start\":1422460800,\"end\":1422502673,\"timezone\":0},\"data_source\":\"contrack_druid_datasource_ds\",\"report_id\":\"78db91f2-7f0f-41ff-8eb9-d9a8cfceda57\",\"pagination\":{\"page\":0,\"size\":10}},\"topn\":{\"metricvalue\":\"clicks\",\"threshold\":10}}";
		String queryParams0 = "{\"data\":[\"clicks\"],\"filters\":{\"$and\":{\"campaign_id\":{\"$match\":\"^d_\"}}},\"group\":[\"campaign_id\",\"aff_sub4\"],\"settings\":{\"time\":{\"start\":1422460800,\"end\":1422502673,\"timezone\":0},\"data_source\":\"contrack_druid_datasource_ds\",\"report_id\":\"78db91f2-7f0f-41ff-8eb9-d9a8cfceda57\",\"pagination\":{\"page\":0,\"size\":10}},\"extract\":[{\"extractDimension\":\"aff_sub4\",\"jsFunction\":\"function(str){return 'aa';}\"},{\"extractDimension\":\"conv_time\",\"jsFunction\":\"function(str){return 'bb';}\"}]}";
//		String queryParams0 = "{\"data\":[\"click\"],\"filters\":{\"$and\":{\"click\":{\"$js\":\"function(x){return x<40;}\"}}},\"group\":[\"offer_id\",\"click_ip\"],\"settings\":{\"time\":{\"start\":1418151611,\"end\":1418756422,\"timezone\":16},\"data_source\":\"ymds_druid_datasource\",\"report_id\":\"56cd1607-e7c3-419b-9bde-aff344cbbccc\",\"pagination\":{\"size\":10000,\"page\":0}},\"sort\":[]}";
//		String queryParams0 = "{\"settings\":{\"report_id\":\"743b411483837d2b48b6aa631bb2982c\",\"return_format\":\"file\",\"data_source\":\"ymds_druid_datasource\",\"time\":{\"start\":1418601600,\"end\":1418688000,\"timezone\":0},\"pagination\":{\"size\":100000,\"page\":0}},\"data\":[\"click\"],\"filters\":{\"$and\":{\"log_tye\":{\"$eq\":1},\"conv_ip\":{\"$match\":\"9(0\"},\"aff_id\":{\"$in\":[90010417]},\"datasource\":{\"$neq\":\"hasoffer\"}}},\"group\":[\"offer_id\",\"aff_sub1\",\"transaction_id\",\"conv_ip\",\"country\",\"browser\",\"aff_id\"]}";
//		String queryParams0 = "{\"settings\":{\"report_id\":\"1402919015\",\"return_format\":\"json\",\"time\":{\"start\":1402876800,\"end\":1402963200,\"timezone\":0},\"data_source\":\"ymds_druid_datasource\",\"pagination\":{\"size\":100000,\"page\":0}},\"filters\":{\"$and\":{}},\"data\":[\"unique_click\",\"click\",\"conversion\"],\"group\":[\"aff_id\"]}";
		// 将请求转化成对象
		ReportParam reportParam = ReportParamFactory.toObject(queryParams0);
		String dataSource = reportParam.getSettings().getData_source();

		// mock metric aggregation
		makeMockObject(dataSource);

		// 公式替换
		String queryParams = Pretreatment.replaceFormula(dataSource, reportParam);

		// 解析
		DruidReportParser parser = WarpInterpreter.convert(queryParams, dataSource, new FileErrorListener());

		QueryContext ctx = QueryFactory.create(parser, queryParams);

		System.out.println("------convert---------");
		System.out.println(ctx.getQuery());
		System.out.println("----toString----------");
		ObjectMapper objectMapper = new DefaultObjectMapper();
		ObjectWriter jsonWriter = objectMapper.writerWithDefaultPrettyPrinter();
		String query_s = jsonWriter.writeValueAsString(ctx.getQuery());
		System.out.println(query_s);

		System.out.println("----------------------");

	}

	private void makeMockObject(String dataSource) {
		DataSourceView viewMock = Mockito.mock(DataSourceView.class);
		Metrics metricsMock = Mockito.mock(Metrics.class);
		Dimentions dimentionsMock = Mockito.mock(Dimentions.class);

		MetricAggTable metricAggTable = new MetricAggTable();
		metricAggTable.init(dataSource);

		DimensionTable dimensionTable = new DimensionTable();
		dimensionTable.init(dataSource);

		Mockito.when(viewMock.metrics()).thenReturn(metricsMock);
		Mockito.when(viewMock.dimentions()).thenReturn(dimentionsMock);

		Mockito.when(metricsMock.getMetricAggTable()).thenReturn(metricAggTable);
		Mockito.when(dimentionsMock.getDimensionTable()).thenReturn(dimensionTable);

		Map<String, DataSourceView> views = ImmutableMap.of(dataSource, viewMock);

		PowerMockito.when(DataSourceViews.getViews()).thenReturn(views);
	}
}
