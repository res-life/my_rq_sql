package com.yeahmobi.datasystem.query.pretreatment;

import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.google.common.collect.ImmutableMap;
import com.yeahmobi.datasystem.query.extensions.DataSourceView;
import com.yeahmobi.datasystem.query.extensions.Dimentions;
import com.yeahmobi.datasystem.query.extensions.Metrics;
import com.yeahmobi.datasystem.query.meta.DimensionTable;
import com.yeahmobi.datasystem.query.meta.MetricAggTable;
import com.yeahmobi.datasystem.query.reportrequest.ReportParam;
import com.yeahmobi.datasystem.query.reportrequest.ReportParamFactory;
import com.yeahmobi.datasystem.query.skeleton.DataSourceViews;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ DataSourceViews.class })
public class PretreatmentTest {

	@Before
	public void before() {
		PowerMockito.mockStatic(DataSourceViews.class);
	}
	
	@Test
	public void test() {
		 String query_param = "{\"currency_type\":\"USD\",\"settings\":{\"report_id\":\"af3a0e3ea19a76157e5cd456b047d2c2\",\"return_format\":\"json\",\"data_source\":\"ymds_druid_datasource\",\"time\":{\"start\":1401811200,\"end\":1401983999,\"timezone\":0},\"pagination\":{\"size\":10000,\"page\":0}},\"group\":[\"aff_id\",\" transaction_id\",\" itvl_hour\",\" currency\"],\"data\":[\"cr\",\"cost\",\"profit\"],\"filters\":{\"$and\":{\"aff_manager\":{\"$in\":[\"90010453\"]}}}}";
		 ReportParam rp=ReportParamFactory.toObject(query_param);
		 String datasouce = rp.getSettings().getData_source();
		 makeMockObject(datasouce);
		 String result=Pretreatment.replaceFormula(datasouce, rp);
		 
		 System.out.println(result);
		
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
