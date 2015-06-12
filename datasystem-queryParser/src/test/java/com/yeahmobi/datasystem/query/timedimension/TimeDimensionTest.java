package com.yeahmobi.datasystem.query.timedimension;

import io.druid.query.aggregation.PostAggregator;
import io.druid.query.groupby.orderby.OrderByColumnSpec;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
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
import com.yeahmobi.datasystem.query.meta.DimensionTable;
import com.yeahmobi.datasystem.query.meta.MetricAggTable;
import com.yeahmobi.datasystem.query.pretreatment.Pretreatment;
import com.yeahmobi.datasystem.query.reportrequest.ReportParam;
import com.yeahmobi.datasystem.query.reportrequest.ReportParamFactory;
import com.yeahmobi.datasystem.query.skeleton.DataSourceViews;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ DataSourceViews.class })
public class TimeDimensionTest {
	
	@Before
	public void before() {
		PowerMockito.mockStatic(DataSourceViews.class);
	}

	@Test
	public void test() throws Exception {
		String queryParams0 = "{\"settings\":{\"report_id\":\"1402919015\",\"return_format\":\"json\",\"time\":{\"start\":1402876800,\"end\":1402963200,\"timezone\":0},\"data_source\":\"ymds_druid_datasource\",\"pagination\":{\"size\":100000,\"page\":0}},\"filters\":{\"$and\":{\"cr\":{\"$gt\":1}}},\"data\":[\"unique_click\",\"click\",\"conversion\",\"cr\",\"cost\",\"profit\"],\"group\":[\"aff_sub1\",\"day\",\"aff_id\",\"offer_id\",\"rpa\"],\"sort\":[{\"orderBy\":\"cr\", \"order\":-1}]}";
		// 将请求转化成对象
		ReportParam reportParam = ReportParamFactory.toObject(queryParams0);
		String dataSource = reportParam.getSettings().getData_source();
		// mock metric aggregation
		makeMockObject(dataSource);
		// 公式替换
		String queryParams = Pretreatment.replaceFormula(dataSource, reportParam);
		// 解析
		DruidReportParser parser = WarpInterpreter.convert(queryParams, dataSource, new FileErrorListener());
		
		List<OrderByColumnSpec> orderByDimensions = parser.columns;
		
		List<String> groupbys = new ArrayList<String>(
				parser.groupByDimensions.keySet());

		List<String> aggregators = new ArrayList<String>(
				parser.aggregators.keySet());

		List<String> postAggregators = new ArrayList<String>();
		for (PostAggregator postAggregator : parser.postAggregators) {
			postAggregators.add(postAggregator.getName());
		}
		
		List<String> timeDemsions = parser.intervalUnits;
		TimeDimension timeDimension = new TimeDimension(parser.getDataSource(),groupbys, aggregators, postAggregators, timeDemsions, orderByDimensions);
		Method method = timeDimension.getClass().getDeclaredMethod("generateCreateTableSql");
		method.setAccessible(true);
		String sql = (String) method.invoke(timeDimension);
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
