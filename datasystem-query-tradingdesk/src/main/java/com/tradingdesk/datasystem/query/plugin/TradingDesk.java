package com.tradingdesk.datasystem.query.plugin;

import java.util.Map;

import com.yeahmobi.datasystem.query.extensions.DataSourcePlugin;
import com.yeahmobi.datasystem.query.extensions.DataSourceView;
import com.yeahmobi.datasystem.query.extensions.Dimentions;
import com.yeahmobi.datasystem.query.extensions.InstalledDataSource;
import com.yeahmobi.datasystem.query.extensions.IntervalTables;
import com.yeahmobi.datasystem.query.extensions.Metrics;
import com.yeahmobi.datasystem.query.jersey.MetricDimentionManager;
import com.yeahmobi.datasystem.query.meta.DimensionDetail;
import com.yeahmobi.datasystem.query.meta.DimensionTable;
import com.yeahmobi.datasystem.query.meta.IntervalTable;
import com.yeahmobi.datasystem.query.meta.IntervalUnit;
import com.yeahmobi.datasystem.query.meta.MetricAggTable;
import com.yeahmobi.datasystem.query.meta.MetricDetail;
import com.tradingdesk.datasystem.query.plugin.LandingPageDataSetHandler;
import com.yeahmobi.datasystem.query.pretreatment.PretreatmentHandler;
import com.yeahmobi.datasystem.query.pretreatment.ReportContext;
import com.yeahmobi.datasystem.query.skeleton.DataSetHandler;
import com.yeahmobi.datasystem.query.skeleton.PostContext;

import ro.fortsoft.pf4j.Extension;
import ro.fortsoft.pf4j.Plugin;
import ro.fortsoft.pf4j.PluginWrapper;

/**
 * plugin of trading desk read file 
 * @author ellis
 * 2014.9.12
 */
public class TradingDesk extends Plugin {

	public TradingDesk(PluginWrapper wrapper) {
		super(wrapper);
	}
	@Extension
	public static class TradingDeskExtention implements InstalledDataSource{

		public DataSourcePlugin getPlugin() {

			final String dataSourceName = "contrack_druid_datasource_ds";
			DataSourceView view = new DataSourceView() {

				public Dimentions dimentions() {
					Dimentions dimentions = new Dimentions() {
						public Map<String, DimensionDetail> getMap() {
							return MetricDimentionManager.getDimension(this.getClass().getClassLoader(), dataSourceName).getTable();
						}

						public DimensionTable getDimensionTable() {
							return MetricDimentionManager.getDimension(this.getClass().getClassLoader(), dataSourceName);
						}
					};
					return dimentions;
				}

				public Metrics metrics() {
					Metrics metrics = new Metrics() {

						public Map<String, MetricDetail> getMap() {
							return MetricDimentionManager.getMetric(this.getClass().getClassLoader(), dataSourceName).getTable();
						}

						public MetricAggTable getMetricAggTable() {
							return MetricDimentionManager.getMetric(this.getClass().getClassLoader(), dataSourceName);
						}
					};
					return metrics;
				}

				public IntervalTables intervalTables() {
					IntervalTables intervalTables = new IntervalTables() {
						
						public Map<String, IntervalUnit> getMap() {
							return IntervalTable.getTable();
						}
					};
					return intervalTables;
				}

				public DataSetHandler getExtraHandler(PostContext reportFeatures) {
					LandingPageDataSetHandler landingPageDataSetHandler = new LandingPageDataSetHandler(reportFeatures);
					return landingPageDataSetHandler;
				}

				public PretreatmentHandler getPreExtraHandler(ReportContext reportContext) {
					return new PretreatmentHandler() {
						@Override
						public void handleRequest(ReportContext reportContext) {
						}
					};
				}

			};

			return new DataSourcePlugin(dataSourceName, view);
		}		
	}
}
