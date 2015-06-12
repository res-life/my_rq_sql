package com.yeahmobi.datasystem.query.landingpage;

/**
 * Created by oscar.gao on 8/4/14.
 */

import io.druid.query.aggregation.PostAggregator;
import io.druid.query.groupby.orderby.OrderByColumnSpec;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;

import javax.persistence.criteria.Join;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.yeahmobi.datasystem.query.akka.cache.db.DbFieldType;
import com.yeahmobi.datasystem.query.exception.ReportDbRuntimeException;
import com.yeahmobi.datasystem.query.skeleton.PostContext;
import com.yeahmobi.datasystem.query.timedimension.TimeDimension;

/**
 *
 */
public class H2InMemoryDbUtil {

	/**
	 * 
	 * @param sql
	 * @param errorMsg
	 * @throws ReportDbRuntimeException
	 */
	public static void executeDbStatement(String sql, String errorMsg) {

		Connection conn = null;
		Statement st = null;

		try {
			conn = H2ConnectionPool.getPool().getConnection();
			st = conn.createStatement();
			st.execute(sql);
		} catch (SQLException e) {
			throw new ReportDbRuntimeException(e, errorMsg);
		} finally {
			closeAutoCloseable(st, "close database statement failed");
			closeAutoCloseable(conn, "close database connection failed");
		}
	}

	/**
	 * query the data from DB with start index and size. index start from 1. SQL
	 * is like: select * from(select rownum r, * from
	 * join_lp_9a61c795_13fc_4140_97be_43ab93524e9b where rownum< 3) where r >=2
	 * 
	 * @param tableName
	 * @param fields
	 * @param startIndex
	 *            从1开始
	 * @param size
	 *            读取几个数据
	 * @param errorMsg
	 * @return
	 * @throws ReportDbRuntimeException
	 */
	public static List<List<Object>> readData(String tableName, String columns, List<String> fields, int startIndex, int size,
			String errorMsg) {

		Connection conn = null;
		Statement st = null;
		ResultSet rs = null;
		try {
			conn = H2ConnectionPool.getPool().getConnection();
			st = conn.createStatement();

			String fieldsStr = StringUtils.join(fields, ",");
			String fieldsWithRowStr = "rownum r," + fieldsStr;
			String sql = String.format("select %s from (select %s from (select * from %s%s) where rownum < %s) where r >= %s", fieldsStr,
					fieldsWithRowStr, tableName, columns, startIndex + size, startIndex);

			rs = st.executeQuery(sql);
			List<List<Object>> rows = new LinkedList<>();
			while (rs.next()) {
				List<Object> row = new LinkedList<>();
				for (int i = 1; i <= fields.size(); ++i) {
					Object o = rs.getObject(i);
					row.add(o);
				}
				rows.add(row);
			}

			return rows;
		} catch (SQLException e) {
			throw new ReportDbRuntimeException(e, errorMsg);
		} finally {
			closeAutoCloseable(rs, "close database result set failed");
			closeAutoCloseable(st, "close database statement failed");
			closeAutoCloseable(conn, "close database connection failed");
		}
	}

	/**
	 * query the data from DB with start index and size. index start from 1. SQL
	 * is like: select * from(select rownum r, * from
	 * join_lp_9a61c795_13fc_4140_97be_43ab93524e9b where rownum< 3) where r >=2
	 * 
	 * @param tableName
	 * @param fields
	 * @param startIndex
	 *            从1开始
	 * @param size
	 *            读取几个数据
	 * @param errorMsg
	 * @return
	 * @throws ReportDbRuntimeException
	 */
	public static List<List<Object>> readDataWithSrotWithPage(String tableName, List<String> fields, int startIndex, int size, String sorter,
			String errorMsg) {
		
		Connection conn = null;
		Statement st = null;
		ResultSet rs = null;
		try {
			conn = H2ConnectionPool.getPool().getConnection();
			st = conn.createStatement();

			String sorterStr = "";
			if(StringUtils.isNotEmpty(sorter)){
				sorterStr = "order by " + sorter;
			}
			
			String fieldsStr = StringUtils.join(fields, ",");
			String sql = String.format("select %s from (select r rr,%s from (select rownum r, %s from %s %s) where r < %s) where rr >= %s", fieldsStr,
					fieldsStr, fieldsStr, tableName, sorterStr, startIndex + size, startIndex);

			rs = st.executeQuery(sql);
			List<List<Object>> rows = new LinkedList<>();
			while (rs.next()) {
				List<Object> row = new LinkedList<>();
				for (int i = 1; i <= fields.size(); ++i) {
					Object o = rs.getObject(i);
					row.add(o);
				}
				rows.add(row);
			}

			return rows;
		} catch (SQLException e) {
			throw new ReportDbRuntimeException(e, errorMsg);
		} finally {
			closeAutoCloseable(rs, "close database result set failed");
			closeAutoCloseable(st, "close database statement failed");
			closeAutoCloseable(conn, "close database connection failed");
		}
	}
	

	/**
	 * 
	 * @param tableName
	 * @param fields
	 * @param filter
	 * @return
	 * @throws ReportDbRuntimeException
	 */
	public static List<List<Object>> selectTable(String tableName, List<String> fields, String filter) {

		Connection conn = null;
		Statement st = null;
		ResultSet rs = null;
		try {
			conn = H2ConnectionPool.getPool().getConnection();
			st = conn.createStatement();

			String fieldsStr = StringUtils.join(fields, ",");

			String sql = String.format("select %s from %s where %s", fieldsStr, tableName, filter);
			rs = st.executeQuery(sql);
			List<List<Object>> rows = new LinkedList<>();
			while (rs.next()) {
				List<Object> row = new LinkedList<>();
				for (int i = 1; i <= fields.size(); ++i) {
					Object o = rs.getObject(i);
					row.add(o);
				}
				rows.add(row);
			}

			return rows;
		} catch (Exception e) {
			throw new ReportDbRuntimeException(e, "query table %s failed", tableName);
		} finally {
			closeAutoCloseable(rs, "close database result set failed");
			closeAutoCloseable(st, "close database statement failed");
			closeAutoCloseable(conn, "close database connection failed");
		}
	}

	/**
	 * 
	 * @param tableName
	 * @param fields
	 * @param filter
	 * @return
	 * @throws ReportDbRuntimeException
	 */
	public static Object selectOneColumn(String sql) {

		Connection conn = null;
		Statement st = null;
		ResultSet rs = null;
		Object ret = null;
		try {
			conn = H2ConnectionPool.getPool().getConnection();
			st = conn.createStatement();
			
			rs = st.executeQuery(sql);
			while (rs.next()) {
				ret = rs.getObject(1);
				break;
			}

			return ret;
		} catch (Exception e) {
			throw new ReportDbRuntimeException(e, "query sql %s failed", sql);
		} finally {
			closeAutoCloseable(rs, "close database result set failed");
			closeAutoCloseable(st, "close database statement failed");
			closeAutoCloseable(conn, "close database connection failed");
		}
		
	}
	
	/**
	 * 
	 * @param tableName
	 * @param errorMsg
	 * @return
	 * @throws ReportDbRuntimeException
	 */
	public static int tableRowSize(String tableName, String errorMsg) {

		String sql = "select count(*) from " + tableName;
		return rowSize(sql, errorMsg);
	}

	/**
	 * 
	 * @param tableName
	 * @param errorMsg
	 * @return
	 * @throws ReportDbRuntimeException
	 */
	public static int tableRowSize(String tableName, String filter, String errorMsg) {

		String sql = "select count(*) from " + tableName + " where " + filter;
		return rowSize(sql, errorMsg);
	}

	/**
	 * 
	 * @param tableName
	 * @param errorMsg
	 * @return
	 * @throws RuntimeException
	 */
	private static int rowSize(String sql, String errorMsg) {

		Connection conn = null;
		Statement st = null;
		ResultSet rs = null;
		try {
			conn = H2ConnectionPool.getPool().getConnection();
			st = conn.createStatement();

			rs = st.executeQuery(sql);

			int size = -1;
			while (rs.next()) {
				size = rs.getInt(1);
			}

			return size;
		} catch (SQLException e) {
			throw new RuntimeException(errorMsg, e);
		} finally {
			closeAutoCloseable(rs, "close database result set failed");
			closeAutoCloseable(st, "close database statement failed");
			closeAutoCloseable(conn, "close database connection failed");
		}
	}

	/**
	 * 
	 * @param autoCloseable
	 * @param errorMsg
	 */
	private static void closeAutoCloseable(AutoCloseable autoCloseable, String errorMsg) {
		if (null != autoCloseable) {
			try {
				autoCloseable.close();
			} catch (Exception e) {
				log.error(errorMsg, e);
			}
		}
	}

	/**
	 * 
	 * @param postContext
	 * @param tableName
	 * @return
	 * @throws ReportDbRuntimeException
	 */
	public static boolean createDbTable(PostContext postContext, String tableName) {
		List<OrderByColumnSpec> orderByDimensions = postContext.getParser().columns;
		
		List<String> groupbys = new ArrayList<String>(
				postContext.getParser().groupByDimensions.keySet());

		List<String> aggregators = new ArrayList<String>(
				postContext.getParser().aggregators.keySet());

		List<String> postAggregators = new ArrayList<String>();
		for (PostAggregator postAggregator : postContext.getParser().postAggregators) {
			postAggregators.add(postAggregator.getName());
		}
		
		List<String> timeDemsions = postContext.getParser().intervalUnits;
		TimeDimension timeDimension = new TimeDimension(postContext.getParser().getDataSource(),groupbys, aggregators, postAggregators, timeDemsions, orderByDimensions);
		timeDimension.processDb(tableName);
		postContext.getFieldTypeMap().put("dbFieldTypeMap", timeDimension.getDbFieldTypeMap());
		postContext.getFieldTypeMap().put("dbGroupFieldTypeMap", timeDimension.getDbGroupFieldTypeMap());
		return true;
	}

	/**
	 * 
	 * @param metaTableName
	 * @param timeTablename
	 * @return
	 * @throws ReportDbRuntimeException
	 */
	public static boolean dropLastTable(String metaTableName, String timeTablename) {
		String sqlMeta = String.format("delete from LP_META where tablename = '%s'", metaTableName);
		String sql = "drop table if exists " + timeTablename;
		String errorMsg = "execute sql failed: " + sql;

		try {
			H2InMemoryDbUtil.executeDbStatement(sqlMeta, errorMsg);
			H2InMemoryDbUtil.executeDbStatement(sql, errorMsg);
			return true;
		} catch (Exception e) {
			log.error("", e);
			return false;
		}
	}

	/**
	 * 
	 * @param tableName
	 * @param fieldsMap
	 * @param rows
	 * @throws ReportDbRuntimeException
	 */
	public static void insertDataList(String tableName, LinkedHashMap<String, DbFieldType> fieldsMap,
			List<List<Object>> rows) {
		List<Object[]> data = Lists.transform(rows, new Function<List<Object>, Object[]>() {
			@Override
			public Object[] apply(List<Object> input) {
				return input.toArray();
			}
		});

		insertData(tableName, fieldsMap, data);
	}

	/**
	 * first read the table definition from the meta table LP_META<br>
	 * then insert data into the landing page table according the table
	 * definition
	 * 
	 * @param dbAddress
	 * @param tableName
	 * @param rows
	 * @throws ReportDbRuntimeException
	 */
	@SuppressWarnings("resource")
	public static void insertData(String tableName, LinkedHashMap<String, DbFieldType> fieldsMap, List<Object[]> rows) {

		Connection conn = null;
		PreparedStatement pstmt = null;

		try {
			conn = H2ConnectionPool.getPool().getConnection();
			String sql = generateInsertSql(tableName, fieldsMap);
			pstmt = conn.prepareStatement(sql);
			for (Object[] row : rows) {
				int i = 0;
				for (Entry<String, DbFieldType> entry : fieldsMap.entrySet()) {

					Object column = row[i];
					DbFieldType type = entry.getValue();
					if (DbFieldType.DECIMAL == type) {
						if(column instanceof String){
							pstmt.setBigDecimal(i + 1, new BigDecimal((String)column));
						}else if(column instanceof Number){
							pstmt.setBigDecimal(i + 1, new BigDecimal(column.toString()));
						}else{
							throw new ReportDbRuntimeException("error type %s, expect long", column.getClass());
						}
						
					} else if (DbFieldType.VARCHAR == type) {
						pstmt.setString(i + 1, column.toString());
					} else if (DbFieldType.BIGINT == type) {
						if(column instanceof String){
							pstmt.setLong(i + 1, Long.parseLong((String) column));
						}else if(column instanceof Number){
							pstmt.setLong(i + 1, ((Number)column).longValue());
						}else{
							throw new ReportDbRuntimeException("error type %s, expect long", column.getClass());
						}
						
					} else {
						throw new ReportDbRuntimeException("error type %s", type);
					}
					++i;
				}
				pstmt.addBatch();
			}

			pstmt.executeBatch();
		} catch (Exception e) {
			throw new ReportDbRuntimeException(e, "failed");
		} finally {
			closeAutoCloseable(pstmt, "close database prepared statement failed");
			closeAutoCloseable(conn, "close database connection failed");
		}
	}

	/**
	 * 
	 * @param tableName
	 * @param length
	 * @return
	 */
	private static String generateInsertSql(String tableName, LinkedHashMap<String, DbFieldType> fieldsMap) {

		List<String> questionMarks = new ArrayList<>();
		for (int i = 0; i < fieldsMap.size(); ++i) {
			questionMarks.add("?");
		}

		String fields = Joiner.on(',').join(fieldsMap.keySet());
		String values = Joiner.on(',').join(questionMarks);
		return String.format("insert into %s(%s) values(%s)", tableName, fields, values);
	}

	private static final Logger log = Logger.getLogger(H2InMemoryDbUtil.class);
}
