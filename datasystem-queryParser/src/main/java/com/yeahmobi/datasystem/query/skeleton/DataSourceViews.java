package com.yeahmobi.datasystem.query.skeleton;

import java.util.List;
import java.util.Map;

import org.rmrodrigues.pf4j.web.PluginManagerHolder;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import com.yeahmobi.datasystem.query.extensions.DataSourceView;
import com.yeahmobi.datasystem.query.extensions.Impala;
import com.yeahmobi.datasystem.query.extensions.InstalledDataSource;
import com.yeahmobi.datasystem.query.jersey.MetricDimentionManager;
import com.yeahmobi.datasystem.query.meta.ImpalaCfg;

/**
 * used to collect all the data source plug-ins<br>
 * 系统启动时， 加载所有的data source 插件
 * 
 */
public class DataSourceViews {

	private static Map<String, DataSourceView> dataSourceViews;

	/**
	 * collect the data source plug-ins
	 */
	public static synchronized void init() {
		
		// get all the extions for the extension point
		List<InstalledDataSource> dataSourceList = PluginManagerHolder
				.getPluginManager().getExtensions(InstalledDataSource.class);
		Builder<String, DataSourceView> dataSourcesBuilder = ImmutableMap
				.builder();
		
		final Impala impalaPlugin = PluginManagerHolder.getPluginManager().getExtensions(Impala.class).get(0);
		ImpalaCfg.setCfgClassLoader(impalaPlugin.getClass().getClassLoader());
		
		MetricDimentionManager.reset();
		for (InstalledDataSource dataSource : dataSourceList) {
			dataSourcesBuilder.put(dataSource.getPlugin().getDataSourceName(),
					dataSource.getPlugin().getDataSourceView());
			
			// 启动的时候调用， 将dimension, metric 配置文件 load 到内存
			MetricDimentionManager.getDimension(dataSource.getClass().getClassLoader(), dataSource.getPlugin().getDataSourceName());
			MetricDimentionManager.getMetric(dataSource.getClass().getClassLoader(), dataSource.getPlugin().getDataSourceName());
		}
		dataSourceViews = dataSourcesBuilder.build();
	}

	/**
	 * get the data source plug-in map key is data source name value is the
	 * plug-in implements
	 * 
	 * @return
	 */
	public static synchronized Map<String, DataSourceView> getViews() {
		return dataSourceViews;
	}
}
