package com.yeahmobi.datasystem.query.akka.cache.db;

import io.druid.query.groupby.orderby.OrderByColumnSpec;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.h2.tools.Server;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
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
import com.yeahmobi.datasystem.query.jersey.ReportServiceResult;
import com.yeahmobi.datasystem.query.meta.DimensionTable;
import com.yeahmobi.datasystem.query.meta.MetricAggTable;
import com.yeahmobi.datasystem.query.meta.MsgType;
import com.yeahmobi.datasystem.query.meta.ReportPage;
import com.yeahmobi.datasystem.query.meta.ReportResult;
import com.yeahmobi.datasystem.query.meta.ReportResult.Entity;
import com.yeahmobi.datasystem.query.pretreatment.Pretreatment;
import com.yeahmobi.datasystem.query.reportrequest.ReportParam;
import com.yeahmobi.datasystem.query.reportrequest.Settings;
import com.yeahmobi.datasystem.query.reportrequest.Settings.Pagination;
import com.yeahmobi.datasystem.query.reportrequest.Settings.Time;
import com.yeahmobi.datasystem.query.skeleton.DataSourceViews;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ DataSourceViews.class })
public class CacheToolL2Test {

	private static org.h2.tools.Server server;

	/**
	 * 启动h2数据库， 并绑定到127.0.0.1<br>
	 * 
	 * @throws SQLException
	 */
	@BeforeClass
	public static void init() throws SQLException {
		server = Server.createTcpServer().start();
	}

	/**
	 * 关闭h2数据库<br>
	 */
	@AfterClass
	public static void destroy() {
		server.shutdown();
	}

	@Before
	public void before() {
		PowerMockito.mockStatic(DataSourceViews.class);
	}

	@Test
	public void test() {

		CacheToolL2 cacheTool = genCacheToolL2();

		// sort 是 off_id 升序
		ReportParam request = genRequest();
		ReportServiceResult result = genResult();

		// mock metric aggregation
		String dataSource = request.getSettings().getData_source();
		makeMockObject(dataSource);

		// 公式替换
		Pretreatment.replaceFormula(dataSource, request);

		// set, 保存的key是公式替换后的key
		Assert.assertTrue(cacheTool.set(request, result.getResult(), false));

		// get
		Object obj = cacheTool.get(request, ReportServiceResult.class);
		Assert.assertNotNull(obj);
	}

	private ReportServiceResult genResult() {

		ReportResult result = new ReportResult();
		Entity data = new Entity();
		List<Object[]> dataList = new ArrayList<Object[]>();

		// 添加表头
		dataList.add(new Object[] { "hour", "aff_id", "click_ip", "click", "cr", "cost", "conversion" });

		// 添加数据
		dataList.add(new Object[] { "1", 1L, 1L, 111L, new BigDecimal(1.11), new BigDecimal(1.111), 1L });
		dataList.add(new Object[] { "2", 2L, 2L, 222L, new BigDecimal(2.22), new BigDecimal(2.222), 2L });

		data.setData(dataList);
		ReportPage page = new ReportPage(0, 100);
		data.setPage(page);
		result.setData(data);
		return new ReportServiceResult(MsgType.success, result);
	}

	private ReportParam genRequest() {
		// group
		List<String> group = new ArrayList<String>();
		group.add("aff_id");
		group.add("hour");
		group.add("click_ip");

		// data
		List<String> data = new ArrayList<String>();
		data.add("click");
		data.add("cr");
		data.add("cost");

		// setting
		Settings settings = new Settings();
		settings.setReport_id("f9fb3fe259c4523ce5439116556eafd1");
		settings.setData_source("ymds_druid_datasource");
		settings.setPagination(new Pagination(1, 1));
		settings.setTime(new Time(1397934000L, 1397934000L, 8));
		settings.setReturn_format("json");

		ReportParam reportParam = new ReportParam();
		reportParam.setSettings(settings);
		reportParam.setGroup(group);

		reportParam.setData(data);
		List<Map<String, Object>> sorts = new ArrayList<>();
		Map<String, Object> sort1 = new HashMap<>();
		sort1.put("orderBy", "aff_id");
		sort1.put("order", 1);
		sorts.add(sort1);

		reportParam.setSort(sorts);

		return reportParam;
	}

	private static CacheToolL2 genCacheToolL2() {
		List<OrderByColumnSpec> orders = Arrays.asList(OrderByColumnSpec.asc("aff_id"), OrderByColumnSpec.desc("hour"));

		return new CacheToolL2("ymds_druid_datasource", Arrays.asList("aff_id", "click_ip"), Arrays.asList("click",
				"cost", "conversion"), Arrays.asList("cr"), Arrays.asList("hour"), orders, false, 0, 2, false);
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
