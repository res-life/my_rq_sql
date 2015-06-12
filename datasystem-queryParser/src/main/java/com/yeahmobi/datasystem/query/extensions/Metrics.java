package com.yeahmobi.datasystem.query.extensions;

import java.util.Map;

import com.yeahmobi.datasystem.query.meta.MetricAggTable;
import com.yeahmobi.datasystem.query.meta.MetricDetail;

/**
 * all the metics for one data source
 *
 */
public interface Metrics {

	/**
	 * get the metric map
	 * @return
	 */
	Map<String, MetricDetail> getMap();
	
	/**
	 * get the MetricAggTable
	 * @return
	 */
	MetricAggTable getMetricAggTable();
}
