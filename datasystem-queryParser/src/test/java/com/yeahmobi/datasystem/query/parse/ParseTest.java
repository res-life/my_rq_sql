package com.yeahmobi.datasystem.query.parse;

import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.google.common.collect.ImmutableMap;
import com.yeahmobi.datasystem.query.antlr4.WarpInterpreter;
import com.yeahmobi.datasystem.query.exception.FileErrorListener;
import com.yeahmobi.datasystem.query.exception.ReportParserException;
import com.yeahmobi.datasystem.query.extensions.DataSourceView;
import com.yeahmobi.datasystem.query.extensions.Dimentions;
import com.yeahmobi.datasystem.query.extensions.Metrics;
import com.yeahmobi.datasystem.query.meta.DimensionTable;
import com.yeahmobi.datasystem.query.meta.MetricAggTable;
import com.yeahmobi.datasystem.query.pretreatment.Pretreatment;
import com.yeahmobi.datasystem.query.reportrequest.ReportParam;
import com.yeahmobi.datasystem.query.reportrequest.ReportParamFactory;
import com.yeahmobi.datasystem.query.skeleton.DataSourceViews;

/**
 * 测试请求转化成druid的请求TestCase
 * 
 * @author chenyi
 * 
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ DataSourceViews.class })
public class ParseTest {

	
	
	
	@Before
	public void before() {
		PowerMockito.mockStatic(DataSourceViews.class);
	}

	/**
	 * 测试: 转化成report parameter对象, 公式替换， parser
	 * 
	 * @throws Exception
	 */
	@Test
	public void testParse() throws Exception {
		String queryParams = "{\"settings\":{\"time\":{\"start\":1413590400,\"end\":1414195200,\"timezone\":0},\"data_source\":\"ymds_druid_datasource\",\"report_id\":\"report-id\",\"pagination\":{\"size\":50000,\"page\":0}},\"group\":[\"day\"],\"data\":[\"click\",\"conversion\"],\"filters\":{\"$and\":{}},\"sort\":[]}";
		//String queryParams = "{\"settings\":{\"report_id\":\"15498146\",\"return_format\":\"json\",\"time\":{\"start\":1407369600,\"end\":1407456000,\"timezone\":0},\"data_source\":\"contrack_druid_datasource_ds\",\"pagination\":{\"size\":10,\"page\":0}},\"data\":[\"clicks\",\"outs\",\"ctr\",\"convs\",\"cr\",\"cost\",\"income\",\"net\",\"roi\"],\"filters\":{\"$and\":{\"offer_id\":{\"$in\":[8386]},\"$or\":{\"offer_id\":{\"$eq\":14044}},\"cr\":{\"$gt\":0}}},\"sort\":[{\"orderBy\":\"offer_id\",\"order\":1},{\"orderBy\":\"ref_site\",\"order\":-1}],\"group\":[\"day\", \"ref_site\",\"offer_id\"]}";
		testParser(queryParams);
	}

	/**
	 * 测试: 转化成report parameter对象, 公式替换， parser
	 * 
	 * @throws Exception
	 */
	@Test
	public void testParse2() throws Exception {

		String queryParams = "{\"settings\":{\"report_id\":\"xxx\",\"return_format\":\"json\",\"time\":{\"start\":1407369600,\"end\":1407456000,\"timezone\":0},\"data_source\":\"contrack_druid_datasource_ds\",\"pagination\":{\"size\":10,\"page\":0}},\"data\":[\"ctr\",\"convs\",\"cr\",\"cost\",\"income\",\"net\",\"roi\"],\"filters\":{\"$and\":{\"offer_id\":{\"$in\":[8386]},\"$or\":{\"offer_id\":{\"$eq\":14044}},\"cr\":{\"$gt\":0}}},\"sort\":[{\"orderBy\":\"offer_id\",\"order\":1},{\"orderBy\":\"ref_site\",\"order\":-1}],\"group\":[\"day\", \"ref_site\",\"offer_id\"]}";
		testParser(queryParams);
	}
	
	private void testParser(String queryParams) throws ReportParserException {
		// 将请求转化成对象
		ReportParam reportParam = ReportParamFactory.toObject(queryParams);
		String dataSource = reportParam.getSettings().getData_source();

		// mock metric aggregation
		makeMockObject(dataSource);

		// 公式替换
		queryParams = Pretreatment.replaceFormula(dataSource,reportParam);


		// 解析
		WarpInterpreter.convert(queryParams, dataSource, new FileErrorListener());
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
