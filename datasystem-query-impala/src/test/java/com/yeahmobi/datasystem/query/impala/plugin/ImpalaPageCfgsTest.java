package com.yeahmobi.datasystem.query.impala.plugin;

import java.lang.reflect.Method;
import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.google.common.collect.ImmutableMap;
import com.yeahmobi.datasystem.query.antlr4.DruidReportParser;
import com.yeahmobi.datasystem.query.antlr4.WarpInterpreter;
import com.yeahmobi.datasystem.query.exception.FileErrorListener;
import com.yeahmobi.datasystem.query.extensions.DataSourceView;
import com.yeahmobi.datasystem.query.extensions.Dimentions;
import com.yeahmobi.datasystem.query.extensions.Metrics;
import com.yeahmobi.datasystem.query.impala.plugin.ImpalaImp.UseImpalaImp;
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
public class ImpalaPageCfgsTest {

	@Before
	public void before() {
		PowerMockito.mockStatic(DataSourceViews.class);
	}
	
	@Test
	public void test1() throws Exception {
		String queryParams0 = "{\"settings\":{\"report_id\":\"1402919015\",\"return_format\":\"json\",\"time\":{\"start\":1402876800,\"end\":1402963200,\"timezone\":0},\"data_source\":\"ymds_druid_datasource\",\"pagination\":{\"size\":100000,\"page\":0}},\"filters\":{\"$and\":{\"aff_sub1\":{\"$match\":\"\\\\\\\\\"}}},\"data\":[\"unique_click\",\"click\",\"conversion\",\"cr\",\"cost\",\"profit\"],\"group\":[\"aff_sub1\",\"day\",\"aff_id\",\"offer_id\",\"rpa\"],\"sort\":[{\"orderBy\":\"cr\", \"order\":-1}]}";
//		String queryParams0 = "{\"settings\":{\"report_id\":\"1402919015\",\"return_format\":\"json\",\"time\":{\"start\":1402876800,\"end\":1402963200,\"timezone\":0},\"data_source\":\"ymds_druid_datasource\",\"pagination\":{\"size\":100000,\"page\":0}},\"filters\":{\"$and\":{}},\"data\":[\"unique_click\",\"click\",\"conversion\",\"cr\"],\"group\":[\"aff_sub1\",\"day\",\"aff_id\",\"offer_id\",\"rpa\"]}";
		// 将请求转化成对象
		ReportParam reportParam = ReportParamFactory.toObject(queryParams0);
		
		String dataSource = reportParam.getSettings().getData_source();

		// mock metric aggregation
		makeMockObject(dataSource);

		String queryParams = Pretreatment.replaceFormula(dataSource, reportParam);
		// 解析
		DruidReportParser parser = WarpInterpreter.convert(queryParams, dataSource, new FileErrorListener());
		LinkedHashMap<String, String> fieldTypeMap = new LinkedHashMap<>();

		UseImpalaImp ui = new UseImpalaImp();
		Method method = ui.getClass().getDeclaredMethod("impalaSql", String.class, DruidReportParser.class, LinkedHashMap.class);
		method.setAccessible(true);
		String sql = (String)method.invoke(ui, dataSource, parser, fieldTypeMap);
		System.out.println(sql);		
	}
	
	@Test
	public void testLpSql() throws Exception {
		String queryParams0 = "{\"settings\":{\"time\":{\"start\":1417021201,\"end\":1417021209,\"timezone\":0},\"process_type\":\"impala,lp\",\"return_format\":\"json\",\"report_id\":\"9998887776644\",\"data_source\":\"contrack_druid_datasource_ds\",\"pagination\":{\"size\":50,\"page\":0}},\"group\":[\"offer_id\",\"device_id\",\"os_id\"],\"data\":[\"clicks\",\"outs\"],\"filters\":{\"$and\":{\"offer_id\":{\"$match\":\"^w\\\\d\"}}},\"sort\":[{\"orderBy\":\"offer_id\",\"order\":1}]}";
//		String queryParams0 = "{\"settings\":{\"report_id\":\"1402919015\",\"return_format\":\"json\",\"time\":{\"start\":1402876800,\"end\":1402963200,\"timezone\":0},\"data_source\":\"ymds_druid_datasource\",\"pagination\":{\"size\":100000,\"page\":0}},\"filters\":{\"$and\":{\"cr\":{\"$gt\":1},\"offer_id\":{\"$eq\":\"203\"}}},\"data\":[\"unique_click\",\"click\",\"conversion\",\"cr\",\"cost\",\"profit\"],\"group\":[\"aff_sub1\",\"day\",\"aff_id\",\"offer_id\",\"rpa\"],\"sort\":[{\"orderBy\":\"cr\", \"order\":-1}]}";
//		String queryParams0 = "{\"settings\":{\"report_id\":\"1402919015\",\"return_format\":\"json\",\"time\":{\"start\":1402876800,\"end\":1402963200,\"timezone\":0},\"data_source\":\"ymds_druid_datasource\",\"pagination\":{\"size\":100000,\"page\":0}},\"filters\":{\"$and\":{}},\"data\":[\"unique_click\",\"click\",\"conversion\",\"cr\"],\"group\":[\"aff_sub1\",\"day\",\"aff_id\",\"offer_id\",\"rpa\"]}";
		// 将请求转化成对象
		ReportParam reportParam = ReportParamFactory.toObject(queryParams0);
		
		String dataSource = reportParam.getSettings().getData_source();

		// mock metric aggregation
		makeMockObject(dataSource);

		String queryParams = Pretreatment.replaceFormula(dataSource, reportParam);
		// 解析
		DruidReportParser parser = WarpInterpreter.convert(queryParams, dataSource, new FileErrorListener());
		LinkedHashMap<String, String> fieldTypeMap = new LinkedHashMap<>();

		UseImpalaImp ui = new UseImpalaImp();
		Method method = ui.getClass().getDeclaredMethod("impalaSqlForTd", String.class, DruidReportParser.class, LinkedHashMap.class);
		method.setAccessible(true);
		String sql = (String)method.invoke(ui, dataSource, parser, fieldTypeMap);
		System.out.println(sql);		
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
