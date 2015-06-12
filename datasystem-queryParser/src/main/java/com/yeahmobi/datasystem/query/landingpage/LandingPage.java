package com.yeahmobi.datasystem.query.landingpage;

/**
 * Created by oscar.gao on 8/4/14.
 */

import io.druid.query.groupby.orderby.OrderByColumnSpec;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.UUID;

import org.apache.log4j.Logger;

import com.yeahmobi.datasystem.query.akka.cache.db.CacheRecord;
import com.yeahmobi.datasystem.query.meta.DimensionTable;
import com.yeahmobi.datasystem.query.meta.LandingPageCfg;
import com.yeahmobi.datasystem.query.meta.MetricAggTable;
import com.yeahmobi.datasystem.query.meta.TableRef;
import com.yeahmobi.datasystem.query.meta.ValueType;
import com.yeahmobi.datasystem.query.skeleton.DataSourceViews;

/**
 * 
 * druid result should be transformed, take following for one example <br>
 * 
 * offer_id device_id clicks outs ctr <br>
 * -1 2 3 0 0.0000 <br>
 * -1 3 4 0 0.0000 <br>
 * 108 3 0 3 0.0000 <br>
 * 109 2 0 2 0.0000 <br>
 * 
 * ------>>> <br>
 * 
 * offer_id device_id clicks outs ctr <br>
 * 108 3 4 3 75% <br>
 * 109 2 3 2 66.7% <br>
 * the lines with offer_id = -1 should be deleted <br>
 * clicks is the value that with same device_id and the offer_id = -1 <br>
 * ctr = outs / clicks <br>
 * 
 * use in-memory database to do the transfer
 */
public class LandingPage {

	private String dataSource;
	private List<String> groupBys;
	private List<String> aggregators;
	private List<String> postAggregators;
	private List<String> timeDimensions;
	private List<OrderByColumnSpec> orderByDimensions;

	private LinkedHashMap<String, String> dbFieldTypeMap = new LinkedHashMap<String, String>();
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
	public LandingPage(String dataSource, List<String> groupBys,
			List<String> aggregators, List<String> postAggregators,
			List<String> timeDimensions, List<OrderByColumnSpec> orderByDimensions) {
		this.dataSource = dataSource;
		this.groupBys = groupBys;
		this.aggregators = aggregators;
		this.postAggregators = postAggregators;
		this.timeDimensions = timeDimensions;
		this.orderByDimensions = orderByDimensions;
	}

	/**
	 * is query valid<br>
	 * should check if it's valid according LandingPageCfg.json
	 * valid group by fields is configured in LandingPageCfg.json
	 * @return
	 */
	public boolean isQueryValid() {
		if ("contrack_druid_datasource_ds".equals(dataSource)
				&& (aggregators.contains("clicks") || aggregators.contains("cost"))
				&& groupBys.contains("offer_id")
				&& (groupBys.size() + timeDimensions.size()) > 1) {

			// read the LandingPageCfg.json to check if query is valid
			// client can define this file
			// the groups in the LandingPageCfg.json is all the valid value
			if (!(LandingPageCfg.getInstance().getGroups()
					.containsAll(groupBys))) {
				return false;
			} else {
				return true;
			}
		} else {
			return false;
		}
	}

	/**
	 * create Landing Page table for storing the result from druid<br>
	 * create join table for storing the secondary transform <br>
	 */
	public void processDb() {

		logger.info("landing page begin to process table");
		// create meta table if not exist
		try {
			// delete timeouted table to avoid in-memory use lots of memory
			deleteTimeoutTable();
			
			// crate landing page table name
			generateUniqueTableName();
			
			// CacheRecord.createMetaTableIfNotExists();
			// over load protect
			// TODO
			// overLoadCheck();

			// create landing page table
			createLandingPageTable();

			// create join table
			createLandingPageJoinTable();

			// save the table info into meta table
			insertLandingPageTableInfoToMetaTable();
		} catch (Exception e) {
			
			logger.error("process db error", e);
			throw e;
		}
		
		logger.info("landing page end to process table " + tableName);
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
			List<List<Object>> rows = H2InMemoryDbUtil.selectTable("LP_META",
					Arrays.asList("tableName, createTime"), "createTime < "
							+ timeoutMillis);
			for (List<Object> row : rows) {
				String tableName = (String) (row.get(0));
				H2InMemoryDbUtil.executeDbStatement("drop table if exists "
						+ tableName, "");
				H2InMemoryDbUtil.executeDbStatement("drop table if exists "
						+ "join_" + tableName, "");
				H2InMemoryDbUtil.executeDbStatement(
						"delete from LP_META where tableName = '" + tableName
								+ "'", "");
				logger.warn("droped timeout table " + tableName);

			}
		} catch (Exception e) {
			String errorMsg = "drop timeout table failed";
			// igore this exception
			logger.error(errorMsg, e);
		}
	}

	private void overLoadCheck() {
		int maxCount = 200;
		try {
			List<List<Object>> rs = H2InMemoryDbUtil.selectTable("LP_META",
					Arrays.asList("cout(*)"), "1=1");
			int count = (int) (rs.get(0).get(0));
			if (count > maxCount) {
				throw new RuntimeException("In-memory databse has " + count
						+ " tables, exceeds the maximum value " + maxCount);
			}
		} catch (Exception e) {
			logger.error(e);
		}
	}


	/**
	 * generate unique table name
	 */
	private void generateUniqueTableName() {
		tableName = "lp_" + UUID.randomUUID().toString().replaceAll("-", "_");
	}

	/**
	 * create landing page table
	 */
	private void createLandingPageTable() {
		// 2 create memory table to save data that broker will insert to
		String createMemoryTableSql = null;
		try {
			createMemoryTableSql = generateCreateTableSql();
			String errorMsg = String.format(
					"can't create the memory table, sql is %s",
					createMemoryTableSql);
			H2InMemoryDbUtil.executeDbStatement(createMemoryTableSql, errorMsg);
		} catch (Exception e) {
			logger.error("landing page, failed to create table sql is " + createMemoryTableSql, e);
			// let web container to handle
			throw e;
		}
	}

	/**
	 * create landing page join table
	 */
	private void createLandingPageJoinTable() {
		// 2 create memory table to save data that broker will insert to
		String createMemoryTableSql = null;
		try {
			createMemoryTableSql = generateCreateJoinTableSql();
			String errorMsg = String.format(
					"can't create the memory table, sql is %s",
					createMemoryTableSql);
			H2InMemoryDbUtil.executeDbStatement(createMemoryTableSql, errorMsg);
		} catch (Exception e) {
			logger.error("landing page, failed to create join table sql is " + createMemoryTableSql, e);
			// let web container to handle
			throw e;
		}
	}

	/**
	 * insert landing page table meta info into meta info table
	 */
	private void insertLandingPageTableInfoToMetaTable() {
		// 3 insert into meta table
		String insertMetaTable = null;
		try {
			String tableFields = generateTableDefine();
			long currentMills = System.currentTimeMillis();
			insertMetaTable = String
					.format("INSERT INTO LP_META(datasource, query, tablename, tablefields, createTime) values('%s','%s','%s','%s', %s)",
							dataSource, "", tableName, tableFields, currentMills);
			String errorMsg = "can't insert landing page meta table";
			H2InMemoryDbUtil.executeDbStatement(insertMetaTable, errorMsg);
		} catch (Exception e) {
			String errorMsg = "landing page, failed to insert into meta table, the sql is " + insertMetaTable;
			logger.error(errorMsg, e);
			// let web container to handle
			throw e;
		}
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

	private String generateCreateJoinTableSql() {

		StringBuffer buffer = generateCreateTableBody(true);

		return "CREATE MEMORY TABLE " + "join_" + tableName
				+ "(timestamp varchar(30)," + buffer.substring(1) + ")";
	}

	private String generateCreateTableSql() {

		StringBuffer buffer = generateCreateTableBody(false);

		return "CREATE MEMORY TABLE " + tableName + "(timestamp varchar(30),"
				+ buffer.substring(1) + ")";
	}

	private StringBuffer generateCreateTableBody(boolean isJoinTable) {
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
				String appendStr = String.format(",%s DECIMAL(50)", group);
				buffer.append(appendStr);
				dbFieldTypeMap.put(group, "DECIMAL");
			} else {
				String msg = String.format(
						"the type of %s is unkonw, please configure", group);
				throw new RuntimeException(msg);
			}
		}

		
		// handle time
		if(!isJoinTable){
			for (String timeUnit : timeDimensions) {
				String appendStr = String.format(",%s VARCHAR(%s)", timeUnit, 30);
				buffer.append(appendStr);
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

	public static void main(String[] args) {
		long twoHourMillis = 1;
		long currentMillis = System.currentTimeMillis();
		long timeoutMillis = currentMillis - twoHourMillis;

		try {
			List<List<Object>> rows = H2InMemoryDbUtil.selectTable("LP_META",
					Arrays.asList("tableName, createTime"), "createTime < "
							+ timeoutMillis);
			for (List<Object> row : rows) {
				String tableName = (String) (row.get(0));
				H2InMemoryDbUtil.executeDbStatement("drop table if exists "
						+ tableName, "");
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

	private static Logger logger = Logger.getLogger(LandingPage.class);

	public List<String> getTimeDimensions() {
		return timeDimensions;
	}

	public void setTimeDimensions(List<String> timeDimensions) {
		this.timeDimensions = timeDimensions;
	}

	public List<OrderByColumnSpec> getOrderByDimensions() {
		return orderByDimensions;
	}

	public void setOrderByDimensions(List<OrderByColumnSpec> orderByDimensions) {
		this.orderByDimensions = orderByDimensions;
	}
}
