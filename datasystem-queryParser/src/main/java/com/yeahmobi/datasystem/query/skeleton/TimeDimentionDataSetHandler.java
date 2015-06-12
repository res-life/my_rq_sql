package com.yeahmobi.datasystem.query.skeleton;

import io.druid.query.aggregation.PostAggregator;
import io.druid.query.groupby.orderby.OrderByColumnSpec;
import io.druid.query.groupby.orderby.OrderByColumnSpec.Direction;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.UUID;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import com.yeahmobi.datasystem.query.landingpage.H2InMemoryDbUtil;
import com.yeahmobi.datasystem.query.meta.DimensionTable;
import com.yeahmobi.datasystem.query.meta.MetricAggTable;
import com.yeahmobi.datasystem.query.meta.TableRef;
import com.yeahmobi.datasystem.query.meta.TimeDimensionCfg;
import com.yeahmobi.datasystem.query.meta.ValueType;

public class TimeDimentionDataSetHandler extends DefaultDataSetHandler{
	private PostContext request;
	
	public TimeDimentionDataSetHandler(PostContext request) {
		this.request = request;
	}

	@Override
	public DataSet processDataSet(DataSet dataSet) {
		
		// 时间维度处理
		DataSet ret = timeDimentionAgg(dataSet);
		
		// 运行处理后续的责任链上的handler
		return super.processDataSet(ret);
	}

	/**
	 * TODO haiwei
	 * @param dataSet
	 */
	private DataSet timeDimentionAgg(DataSet dataSet) {
		DataBaseDataSet dataBaseDataSet = (DataBaseDataSet)dataSet;
		String timeTablename = dataBaseDataSet.getTableName();
		String groupTableName = "group_" + timeTablename;
		createTimeDimensionGroupTable(timeTablename);
		createIndex(timeTablename);
		groupTable(timeTablename,groupTableName);
		dropLandingPageTable(timeTablename);
		dataBaseDataSet.setLastTableName(groupTableName);
		return dataBaseDataSet;
	}
	
	private boolean groupTable(String timeTablename,String groupTableName) {
		LinkedHashMap<String, String> metrics = null;
		String datasource = request.getParser().getDataSource();
		if(datasource.equalsIgnoreCase("contrack_druid_datasource_ds") || "eve_druid_datasource_ds".equalsIgnoreCase(datasource)){
			metrics = TimeDimensionCfg.getInstance()
					.getTradingDeskTimeMetrics();
		}else if(datasource.equalsIgnoreCase("ymds_druid_datasource")){
			metrics = TimeDimensionCfg.getInstance()
					.getYeahMobiMetrics();
		}else{
			throw new RuntimeException("do timeDimension handle ,this datasource not exist");
		}

		List<String> postAggregators = new ArrayList<String>();
		for (PostAggregator postAggregator : request.getParser().postAggregators) {
			postAggregators.add(postAggregator.getName());
		}
		String groupSql = generateGroupSql(timeTablename, groupTableName, 
				request.getParser().intervalUnits, new ArrayList<String>(
				request.getParser().groupByDimensions.keySet()),
				new ArrayList<String>(request.getParser().aggregators.keySet()), postAggregators,metrics,
				request.getParser().columns);

		try {
			String errorMsg = String.format("group table failed, sql is "
					+ groupSql);
			logger.debug("group table sql is: " + groupSql);
			H2InMemoryDbUtil.executeDbStatement(groupSql, errorMsg);
			return true;
		} catch (Exception e) {
			logger.error("", e);
			return false;
		}
	}
	
	private static String generateGroupSql(String timeTable, String groupTable,List<String> timeGroups ,
			List<String> groupBys, List<String> aggregators,
			List<String> postAggregators, LinkedHashMap<String, String> metrics,List<OrderByColumnSpec> orderByDimensions){
		HashMap<String, String> timeMap = new HashMap<String, String>();
//		timeMap.put("year", "year(cast(timestamp as timestamp))");
//		timeMap.put("month", "substr(monthname(cast(timestamp as timestamp)),0,3)");
//		timeMap.put("month", "casewhen(month(cast(timestamp as timestamp))<10,concat('0',month(cast(timestamp as timestamp))),cast(month(cast(timestamp as timestamp)) as char))");
//		timeMap.put("week", "concat(casewhen(month(cast(timestamp as timestamp))<10,concat('0',month(cast(timestamp as timestamp))),cast(month(cast(timestamp as timestamp)) as char)),'-',weekofmonth(substr(timestamp,0,22)))");
//		timeMap.put("day", "cast(cast(timestamp as timestamp) as date)");
//		timeMap.put("hour", "hour(cast(substr(timestamp,0,23) as timestamp))");
		
		timeMap.put("year", "year(cast(substr(timestamp,0,23) as timestamp))");
		timeMap.put("month", "casewhen(month(cast(substr(timestamp,0,23) as timestamp))<10,concat('0',month(cast(substr(timestamp,0,23) as timestamp))),cast(month(cast(substr(timestamp,0,23) as timestamp)) as char))");
		timeMap.put("week", "concat(casewhen(month(cast(substr(timestamp,0,23) as timestamp))<10,concat('0',month(cast(substr(timestamp,0,23) as timestamp))),cast(month(cast(substr(timestamp,0,23) as timestamp)) as char)),'-',weekofmonth(substr(timestamp,0,22)))");
		timeMap.put("day", "cast(cast(substr(timestamp,0,23) as timestamp) as date)");
		timeMap.put("hour", "hour(cast(substr(timestamp,0,23) as timestamp))");
		
		//group 的内容
		StringBuilder timeGroup = new StringBuilder();
		for (int i = 0; i < timeGroups.size()-1; i++) {
			timeGroup.append(timeGroups.get(i));
			timeGroup.append(",");
		}
		timeGroup.append(timeGroups.get(timeGroups.size()-1));
		
		//select 的内容
		StringBuilder fieldStrs = new StringBuilder();
		
		List<String> timeMaps = new ArrayList<String>();
		for (String string : timeGroups) {
			timeMaps.add(timeMap.get(string)+" as "+string);
		}
		String timeStrs = StringUtils.join(timeMaps, ",");
		fieldStrs.append(timeStrs);
		
		if(groupBys.size() > 0){
			timeGroup.append(",");
			fieldStrs.append(",");
			String groupByStrs = StringUtils.join(groupBys, ",");
			timeGroup.append(groupByStrs);
			fieldStrs.append(groupByStrs);
		}
		
		List<String> sumAggregators = new ArrayList<String>();
		if(aggregators.size() > 0){
			fieldStrs.append(",");
			for (int i = 0; i < aggregators.size(); i++) {
				sumAggregators.add("sum("+aggregators.get(i)+")" + " as "+aggregators.get(i));
			}
			String aggregatorStrs = StringUtils.join(sumAggregators, ",");
			fieldStrs.append(aggregatorStrs);
		}
		
		if(postAggregators.size() > 0){
			List<String> fieldpostAggregators = new ArrayList<String>();;
			for (String f : postAggregators) {
				fieldpostAggregators.add(metrics.get(f) + " as " + f);
			}
			fieldStrs.append(",");
			String postAggregatorStrs = StringUtils.join(fieldpostAggregators, ",");
			fieldStrs.append(postAggregatorStrs);
		}
		
		//order by 条件
		StringBuffer strOrderCol = new StringBuffer();
		if(orderByDimensions.size() > 0){
			strOrderCol.append(" order by ");
			StringBuffer strOrderColDimens = new StringBuffer();
			for (OrderByColumnSpec ordercol : orderByDimensions) {
				strOrderColDimens.append(",");
				strOrderColDimens.append(ordercol.getDimension());
				if(ordercol.getDirection().compareTo(Direction.ASCENDING) == 0){
					strOrderColDimens.append(" asc");
				}else{
					strOrderColDimens.append(" desc");
				}
			}
			strOrderCol.append(strOrderColDimens.substring(1));
		}
		String groupSql = String
				.format("insert into %s ( select %s from %s group by %s %s)",
						groupTable,fieldStrs.toString(),timeTable,timeGroup.toString(),strOrderCol.toString());
		return groupSql;
	}
	
	private void dropLandingPageTable(String timeTablename) {

		String sql = "drop table if exists " + timeTablename;
		String errorMsg = "execute sql failed: " + sql;

		try {
			H2InMemoryDbUtil.executeDbStatement(sql, errorMsg);
		} catch (Exception e) {
			logger.error("", e);
		}
	}
	
	private void createTimeDimensionGroupTable(String tableName) {
		// 2 create memory table to save data that broker will insert to
		
		String createMemoryTableSql = generateCreateGroupTableSql(tableName);
		logger.debug("create time table is" + createMemoryTableSql);
		String errorMsg = String.format(
				"can't create the memory table, sql is %s",
				createMemoryTableSql);
		H2InMemoryDbUtil.executeDbStatement(createMemoryTableSql, errorMsg);
	}
	
	private String generateCreateGroupTableSql(String tableName) {

		List<String> postAggregators = new ArrayList<String>();
		for (PostAggregator postAggregator : request.getParser().postAggregators) {
			postAggregators.add(postAggregator.getName());
		}
		StringBuffer buffer = generateCreateTableBody(
				new ArrayList<String>(request.getParser().groupByDimensions.keySet()),
				new ArrayList<String>(request.getParser().aggregators.keySet()), postAggregators);
		StringBuffer timeStrs = new StringBuffer();
		for (String string : request.getParser().intervalUnits) {
			timeStrs.append(",");
			timeStrs.append(string);
			timeStrs.append(" varchar(20)");
		}
		timeStrs.append(",");

		return "CREATE MEMORY TABLE " + "group_" + tableName
				+ "(" + timeStrs.substring(1) + buffer.substring(1) + ")";
	}
	
	private StringBuffer generateCreateTableBody(ArrayList<String> groupBys,ArrayList<String> aggregators,
			                                     List<String> postAggregators) {
		StringBuffer buffer = new StringBuffer();

		String oridatasource = request.getParser().getDataSource();
        DimensionTable dimensionTable = DataSourceViews.getViews().get(oridatasource).dimentions().getDimensionTable();
        MetricAggTable metricAggTable = DataSourceViews.getViews().get(oridatasource).metrics().getMetricAggTable();


		// handle group
		for (String group : groupBys) {
			ValueType type = dimensionTable.getValueType(group);
			if (ValueType.NUMBER == type || ValueType.INTEGER == type) {
				String appendStr = String.format(",%s BIGINT", group);
				buffer.append(appendStr);
			} else if (ValueType.STRING == type) {
				int maxLength = dimensionTable.getTable().get(group).getMaxLength();
				String appendStr = String.format(",%s VARCHAR(%s)", group, maxLength);
				buffer.append(appendStr);
			} else if (ValueType.DECIMAL == type) {
				String appendStr = String.format(",%s DECIMAL(50,3)", group);
				buffer.append(appendStr);
			} else {
				String msg = String.format(
						"the type of %s is unkonw, please configure", group);
				throw new RuntimeException(msg);
			}
		}

		// handle the aggregators
		for (String aggregator : aggregators) {
			int precision = metricAggTable.getAggPrecision(aggregator);
			if (precision > 0) {
				String appendStr = String.format(",%s DECIMAL(30,%s)",
						aggregator, precision);
				buffer.append(appendStr);
			} else {
				String appendStr = String.format(",%s BIGINT", aggregator,
						precision);
				buffer.append(appendStr);
			}
		}

		// handle the post aggregators
		for (String postAggregator : postAggregators) {
			int precision = metricAggTable.getAggPrecision(postAggregator);
			if (precision > 0) {
				String appendStr = String.format(",%s DECIMAL(30,%s)",
						postAggregator, precision);
				buffer.append(appendStr);
			} else {
				String appendStr = String.format(",%s BIGINT", postAggregator,
						precision);
				buffer.append(appendStr);
			}
		}

		return buffer;
	}
	
	private boolean createIndex(String tableName) {
		try {
				String sql = String.format("create index index_%s on %s(%s)",
						UUID.randomUUID().toString().replaceAll("-","_"),tableName,"timestamp");
				String errorMsg = String.format("create index failed, sql is "
						+ sql);
				H2InMemoryDbUtil.executeDbStatement(sql, errorMsg);
			
			return true;
		} catch (Exception e) {
			logger.error("create index failed ", e);
			return false;
		}
	}
	
	private static final Logger logger = Logger.getLogger(TimeDimentionDataSetHandler.class);
}
