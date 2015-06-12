package com.yeahmobi.datasystem.query.timedimension;

import io.druid.query.groupby.orderby.OrderByColumnSpec;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import com.yeahmobi.datasystem.query.akka.cache.db.CacheRecord;
import com.yeahmobi.datasystem.query.akka.cache.db.CacheStatus;
import com.yeahmobi.datasystem.query.landingpage.H2InMemoryDbUtil;
import com.yeahmobi.datasystem.query.meta.DimensionTable;
import com.yeahmobi.datasystem.query.meta.MetricAggTable;
import com.yeahmobi.datasystem.query.meta.ValueType;
import com.yeahmobi.datasystem.query.skeleton.DataSourceViews;

/**
 * 时间维度的前处理，用于创建表等操作
 * @author ellis
 * @version 1.0
 */
public class TimeDimension {
	private static Logger logger = Logger.getLogger(TimeDimension.class);
	
	private String dataSource;
	private List<String> groupBys;
	private List<String> aggregators;
	private List<String> postAggregators;
	private List<String> timeDimensions;
	private List<OrderByColumnSpec> orderByDimensions;

	public LinkedHashMap<String, String> dbFieldTypeMap = new LinkedHashMap<String, String>();
	public LinkedHashMap<String, String> dbGroupFieldTypeMap = new LinkedHashMap<String, String>();
	private String tableName;

	/**
	 * constructor
	 * 
	 * @param dataSource
	 *            the data source
	 * @param metrics
	 *            metrics
	 * @param groupSet
	 *            groups
	 * @param druidQuery
	 */
	public TimeDimension(String dataSource,List<String> groupBys,
			List<String> aggregators, List<String> postAggregators,
			List<String> timeDimensions,List<OrderByColumnSpec> orderByDimensions) {
		this.dataSource = dataSource;
		this.groupBys = groupBys;
		this.aggregators = aggregators;
		this.postAggregators = postAggregators;
		this.timeDimensions = timeDimensions;
		this.orderByDimensions = orderByDimensions;
	}

	/**
	 * 创建存放druid数据的数据库表
	 */
	public void processDb(String tableName) {

		// create meta table if not exist
		// CacheRecord.createMetaTableIfNotExists();
		
		//h2里面的时间函数与业务不匹配，重新写了
		//createWeekOfMonthFunction();

		// delete timeouted table to avoid in-memory use lots of memory
		// 由于要做cache，这个操作可能要交给cache处的代码或是同步时间。目前是两个小时
		deleteTimeoutTable();

		this.tableName = tableName;

		// 创建基础表，即存放druid出来结果的那张表
		createTimeDimensionTable();

		// 把数据类型存放在meta表中，这样在druid找到要往数据库中插的数据类型
		insertLandingPageTableInfoToMetaTable();
	}

	/**
	 * the landing page table
	 * 
	 * @return
	 */
	public String getTableName() {
		return tableName;
	}

	/**
	 * get the table field type map
	 * 
	 * @return
	 */
	public LinkedHashMap<String, String> getDbFieldTypeMap() {
		return dbFieldTypeMap;
	}

	/**
	 * delete timeout table.<br>
	 * for some exception scenario, like druid broker node or real query node
	 * restart<br>
	 * the table in database will not delete<br>
	 * if the LP table created exceeds 2 hour, will drop it<br>
	 * the memory is critical resource.
	 */
	private void deleteTimeoutTable() {

		long twoHourMillis = 2 * 3600 * 1000;
		long currentMillis = System.currentTimeMillis();
		long timeoutMillis = currentMillis - twoHourMillis;

		try {
			
			// 时间维度和LP处理没有设置CACHESTATUS， 可以删除这种临时表
			String filter = String.format("createTime < %s and CACHESTATUS = ''", timeoutMillis);
			List<List<Object>> rows = H2InMemoryDbUtil.selectTable("LP_META",
					Arrays.asList("tableName, createTime"), filter);
			for (List<Object> row : rows) {
				String tableName = (String) (row.get(0));
				H2InMemoryDbUtil.executeDbStatement("drop table if exists "
						+ tableName, "");
				H2InMemoryDbUtil.executeDbStatement("drop table if exists "
						+ "group_" + tableName, "");
				H2InMemoryDbUtil.executeDbStatement("drop table if exists "
						+ "join_" + tableName, "");
				H2InMemoryDbUtil.executeDbStatement(
						"delete from LP_META where tableName = '" + tableName
								+ "'", "");
				logger.warn("droped timeout table " + tableName);

			}
		} catch (Exception e) {
			String errorMsg = "drop timeout table failed";
			logger.error(errorMsg, e);
		}
	}
	
	private void createWeekOfMonthFunction() {
		String createFunction = "CREATE ALIAS IF NOT EXISTS WEEKOFMONTH AS $$ import java.util.Calendar; import org.joda.time.DateTime; @CODE int weekOfMonth(String str) throws Exception{ Calendar c = Calendar.getInstance(); c.setTime(DateTime.parse(str).toDate()); int week = c.get(Calendar.WEEK_OF_MONTH); return week; } $$ ;";
		String errorMsg = "can't create h2 function";
		H2InMemoryDbUtil.executeDbStatement(createFunction,
				errorMsg);
	}
	

	/**
	 * create landing page table
	 */
	private void createTimeDimensionTable() {
		// 2 create memory table to save data that broker will insert to
		String createMemoryTableSql = generateCreateTableSql();
		String errorMsg = String.format(
				"can't create the memory table, sql is %s",
				createMemoryTableSql);
		H2InMemoryDbUtil.executeDbStatement(createMemoryTableSql, errorMsg);
		
		logger.debug("create landing page table sql is: " + createMemoryTableSql);
	}

	
	/**
	 * insert landing page table meta info into meta info table
	 */
	private void insertLandingPageTableInfoToMetaTable() {
		// 3 insert into meta table

		String tableFields = generateTableDefine();
		long currentMills = System.currentTimeMillis();
		String datasource = dataSource;
		String insertMetaTable = String
				.format("INSERT INTO LP_META(datasource, query, tablename, tablefields, createTime, cachestatus) values('%s','%s','%s','%s', %s, '%s')",
						datasource, "", tableName, tableFields, currentMills, CacheStatus.NOT_READY);
		String errorMsg = "can't insert timeing page meta table";
		H2InMemoryDbUtil.executeDbStatement(insertMetaTable, errorMsg);
	}

	private String generateTableDefine() {
		StringBuffer buffer = new StringBuffer();

		for (Entry<String, String> pair : dbFieldTypeMap.entrySet()) {
			String field = String.format(",%s=%s", pair.getKey(),
					pair.getValue());
			buffer.append(field);
		}

		return buffer.substring(1);
	}

	private String generateCreateTableSql() {

		StringBuffer buffer = generateCreateTableBody();

		return "CREATE MEMORY TABLE " + tableName + "(timestamp varchar(30),"
				+ buffer.substring(1) + ")";
	}

	private StringBuffer generateCreateTableBody() {
		StringBuffer buffer = new StringBuffer();

		String datasource = dataSource;
        DimensionTable dimensionTable = DataSourceViews.getViews().get(datasource).dimentions().getDimensionTable();
        MetricAggTable metricAggTable = DataSourceViews.getViews().get(datasource).metrics().getMetricAggTable();

		dbFieldTypeMap.put("timestamp", "VARCHAR");

		// handle group
		for (String group : groupBys) {
			ValueType type = dimensionTable.getValueType(group);
			if (ValueType.NUMBER == type || ValueType.INTEGER == type) {
				String appendStr = String.format(",%s BIGINT", group);
				buffer.append(appendStr);
				dbFieldTypeMap.put(group, "BIGINT");
			} else if (ValueType.STRING == type) {
				int maxLength = dimensionTable.getTable().get(group).getMaxLength();
				String appendStr = String.format(",%s VARCHAR(%s)", group, maxLength);
				buffer.append(appendStr);
				dbFieldTypeMap.put(group, "VARCHAR");
			} else if (ValueType.DECIMAL == type) {
				String appendStr = String.format(",%s DECIMAL(50,3)", group);
				buffer.append(appendStr);
				dbFieldTypeMap.put(group, "DECIMAL");
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
				dbFieldTypeMap.put(aggregator, "DECIMAL");
			} else {
				String appendStr = String.format(",%s BIGINT", aggregator,
						precision);
				buffer.append(appendStr);
				dbFieldTypeMap.put(aggregator, "BIGINT");
			}
		}

		// handle the post aggregators
		for (String postAggregator : postAggregators) {
			int precision = metricAggTable.getAggPrecision(postAggregator);
			if (precision > 0) {
				String appendStr = String.format(",%s DECIMAL(30,%s)",
						postAggregator, precision);
				buffer.append(appendStr);
				dbFieldTypeMap.put(postAggregator, "DECIMAL");
			} else {
				String appendStr = String.format(",%s BIGINT", postAggregator,
						precision);
				buffer.append(appendStr);
				dbFieldTypeMap.put(postAggregator, "BIGINT");
			}
		}

		dbGroupFieldTypeMap.putAll(dbFieldTypeMap);
		dbGroupFieldTypeMap.remove("timestamp");
		for (String string : timeDimensions) {
			dbGroupFieldTypeMap.put(string, "VARCHAR");
		}
		
		return buffer;
	}

	/**
	 * Getter
	 * 
	 * @return
	 */
	public List<String> getGroupBys() {
		return groupBys;
	}

	/**
	 * Getter
	 * 
	 * @return
	 */
	public List<String> getAggregators() {
		return aggregators;
	}

	/**
	 * Getter
	 * 
	 * @return
	 */
	public List<String> getPostAggregators() {
		return postAggregators;
	}
	
	public List<String> getTimeDimensions() {
		return timeDimensions;
	}

	public LinkedHashMap<String, String> getDbGroupFieldTypeMap() {
		return dbGroupFieldTypeMap;
	}

	public List<OrderByColumnSpec> getOrderByDimensions() {
		return orderByDimensions;
	}

	public void setOrderByDimensions(List<OrderByColumnSpec> orderByDimensions) {
		this.orderByDimensions = orderByDimensions;
	}
	
}
