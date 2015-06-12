package com.yeahmobi.datasystem.query.jersey;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.yeahmobi.datasystem.query.meta.DimensionTable;
import com.yeahmobi.datasystem.query.meta.MetricAggTable;
import com.yeahmobi.datasystem.query.serializer.ObjectSerializer;

public class MetricDimentionManager {

	public static final Map<String, DimensionTable> DIMENSIONS = new ConcurrentHashMap<>();
	public static final Map<String, MetricAggTable> METRICS = new ConcurrentHashMap<>();
	public static final Map<String, ClassLoader> dimensionsClassLoader = new ConcurrentHashMap<>();
	public static final Map<String, ClassLoader> metricClassLoader = new ConcurrentHashMap<>();

	public static void reset() {
		DIMENSIONS.clear();
		METRICS.clear();
		dimensionsClassLoader.clear();
		metricClassLoader.clear();
	}

	public static DimensionTable getDimension(ClassLoader classLoader, String dataSourceName) {

		if (DIMENSIONS.containsKey(dataSourceName)) {
			return DIMENSIONS.get(dataSourceName);
		} else {
			DimensionTable dimensionTable = new DimensionTable(classLoader);
			dimensionTable.init(dataSourceName);

			if (null != dimensionTable) {

				// 保存cache
				DIMENSIONS.put(dataSourceName, dimensionTable);
				dimensionsClassLoader.put(dataSourceName, classLoader);
			}

			return dimensionTable;
		}
	}

	public static DimensionTable getDimension(String dataSourceName) {

		return DIMENSIONS.get(dataSourceName);
	}

	public static void setDimension(DimensionTable table) {

		DIMENSIONS.put(table.getDataSource(), table);
		ObjectSerializer.write(table.getDataSource() + "_dimention.json", table.getTable(),
				dimensionsClassLoader.get(table.getDataSource()));
	}

	public static MetricAggTable getMetric(ClassLoader classLoader, String dataSourceName) {
		if (METRICS.containsKey(dataSourceName)) {
			return METRICS.get(dataSourceName);
		} else {
			MetricAggTable metricAggTable = new MetricAggTable(classLoader);
			metricAggTable.init(dataSourceName);

			if (null != metricAggTable) {
				// 保存cache
				METRICS.put(dataSourceName, metricAggTable);
				metricClassLoader.put(dataSourceName, classLoader);
			}

			return metricAggTable;
		}
	}

	public static MetricAggTable getMetric(String dataSourceName) {
		return METRICS.get(dataSourceName);
	}

	public static void setMetric(MetricAggTable metric) {

		METRICS.put(metric.getDataSource(), metric);
		ObjectSerializer.write(metric.getDataSource() + "_metric.json", metric.getTable(),
				metricClassLoader.get(metric.getDataSource()));
	}
}
