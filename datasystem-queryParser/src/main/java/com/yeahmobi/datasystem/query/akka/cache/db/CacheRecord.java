package com.yeahmobi.datasystem.query.akka.cache.db;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map.Entry;

import jersey.repackaged.com.google.common.base.Preconditions;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.yeahmobi.datasystem.query.landingpage.H2InMemoryDbUtil;

/**
 * 表示LP_META表里的一行<BR>
 * 这个表保存元信息<BR>
 */
public class CacheRecord {

	// 元信息表名称
	private static final String META_TABLE_NAME = "LP_META";

	/**
	 * Getter
	 * @return
	 */
	public static String getMetaTableName() {
		return META_TABLE_NAME;
	}

	// data source
	private String dataSource;

	// query 去掉 sort和分页信息
	private String query;

	// id, 这个值是惟一的. 
	// 注意与数据库表的tablename对应
	private String id;

	// 结果表的字段元信息
	// 数据库里面格式是： aField=VARCHAR,bField=DECIMAL, 注意没有空格
	private LinkedHashMap<String, DbFieldType> tableFields;

	// 这个cache entry 创建的时间
	private long createTime;

	// 这个query结果保存的表名
	private String resultTable;

	// cache 的状态
	private CacheStatus cacheStatus;

	// cache 过期的时间
	private long timeoutTime;

	// result 表的容量，行*列, 不考虑每个字段的大小。
	private long capacity;
	
	private String isFullData = "false";

	/**
	 * 返回数据库表的列名list
	 * @return
	 */
	public static List<String> getDbFields() {
		return ImmutableList.of("datasource", "query", "tablename", "tablefields", "createtime", "resulttable",
				"cachestatus", "timeouttime", "capacity");
	}

	/**
	 * 返回数据库表的列的类型map
	 * @return
	 */
	public static LinkedHashMap<String, DbFieldType> getFieldTypeMap() {
		LinkedHashMap<String, DbFieldType> map = new LinkedHashMap<>();
		map.put("datasource", DbFieldType.VARCHAR);
		map.put("query", DbFieldType.VARCHAR);
		map.put("tablename", DbFieldType.VARCHAR);
		map.put("tablefields", DbFieldType.VARCHAR);
		map.put("createtime", DbFieldType.BIGINT);
		map.put("resulttable", DbFieldType.VARCHAR);
		map.put("cachestatus", DbFieldType.VARCHAR);
		map.put("timeouttime", DbFieldType.BIGINT);
		map.put("capacity", DbFieldType.BIGINT);
		map.put("isfulldata", DbFieldType.VARCHAR);
		
		return map;
	}

	/**
	 * create table<br>
	 * 注意增加了四列： resulttable, cachestatus, timeouttime, capacity<br>
	 * 升级时， 需要将此表drop， 会自动重创建<br>
	 * TODO, 后续放到web server启动的地方<br>
	 */
	public static void createMetaTableIfNotExists() {
		// 1 if not initialize the meta landing page table, create the table
		String createLandingPageMetaTable = String
				.format("CREATE MEMORY TABLE IF NOT EXISTS %s(datasource varchar(100), query varchar(5120), tablename varchar(100), tablefields varchar(2000), createTime bigint, resulttable varchar(100) , cachestatus varchar(20), timeouttime bigint, capacity bigint, isfulldata varchar(5))",
						META_TABLE_NAME);
		String errorMsg = "can't create meta table or index";
		String createIndexSql = String.format("create index if not exists lp_meta_table_name_index on %s(tablename)", META_TABLE_NAME);
		
		H2InMemoryDbUtil.executeDbStatement(createLandingPageMetaTable, errorMsg);
		H2InMemoryDbUtil.executeDbStatement(createIndexSql, errorMsg);		
	}

	public void check(){
		Preconditions.checkNotNull(dataSource);
		Preconditions.checkNotNull(query);
		Preconditions.checkNotNull(id);
		Preconditions.checkNotNull(tableFields);
		Preconditions.checkNotNull(createTime);
		Preconditions.checkNotNull(resultTable);
		Preconditions.checkNotNull(cacheStatus);
		Preconditions.checkNotNull(timeoutTime);
		Preconditions.checkNotNull(capacity);
	}
	/**
	 * Getter
	 * 
	 * @return
	 */
	public String getDataSource() {
		return dataSource;
	}

	/**
	 * Setter
	 * 
	 * @param dataSource
	 */
	public void setDataSource(String dataSource) {
		this.dataSource = dataSource;
	}

	/**
	 * Getter
	 * 
	 * @return
	 */
	public String getQuery() {
		return query;
	}

	/**
	 * Setter
	 * 
	 * @param query
	 */
	public void setQuery(String query) {
		this.query = query;
	}

	/**
	 * Getter
	 * 
	 * @return
	 */
	public String getId() {
		return id;
	}

	/**
	 * Setter
	 * 
	 * @param id
	 */
	public void setId(String id) {
		this.id = id;
	}

	/**
	 * Getter
	 * 
	 * @return
	 */
	public LinkedHashMap<String, DbFieldType> getTableFields() {
		return tableFields;
	}

	/**
	 * Setter
	 * 
	 * @param tableFields
	 */
	public void setTableFields(LinkedHashMap<String, DbFieldType> tableFields) {
		this.tableFields = tableFields;
	}

	/**
	 * Getter
	 * 
	 * @return
	 */
	public long getCreateTime() {
		return createTime;
	}

	/**
	 * Setter
	 * 
	 * @param createTime
	 */
	public void setCreateTime(long createTime) {
		this.createTime = createTime;
	}

	/**
	 * Getter
	 * 
	 * @return
	 */
	public String getResultTable() {
		return resultTable;
	}

	/**
	 * Setter
	 * 
	 * @param resultTable
	 */
	public void setResultTable(String resultTable) {
		this.resultTable = resultTable;
	}

	/**
	 * Getter
	 * 
	 * @return
	 */
	public CacheStatus getCacheStatus() {
		return cacheStatus;
	}

	/**
	 * Setter
	 * 
	 * @param cacheStatus
	 */
	public void setCacheStatus(CacheStatus cacheStatus) {
		this.cacheStatus = cacheStatus;
	}

	/**
	 * Getter
	 * 
	 * @return
	 */
	public long getTimeoutTime() {
		return timeoutTime;
	}

	/**
	 * Setter
	 * 
	 * @param timeoutTime
	 */
	public void setTimeoutTime(long timeoutTime) {
		this.timeoutTime = timeoutTime;
	}

	/**
	 * Getter
	 * 
	 * @return
	 */
	public long getCapacity() {
		return capacity;
	}

	/**
	 * Setter
	 * 
	 * @param capacity
	 */
	public void setCapacity(long capacity) {
		this.capacity = capacity;
	}

	/**
	 * 私有构造函数
	 */
	private CacheRecord() {
	}

	
	public String getIsFullData() {
		return isFullData;
	}

	public void setIsFullData(String isFullData) {
		this.isFullData = isFullData;
	}

	/**
	 * return Builder
	 * 
	 * @return
	 */
	public static CacheEntryBuilder builder() {
		return new CacheEntryBuilder();
	}

	/**
	 * Builder class
	 * 
	 */
	public static class CacheEntryBuilder {
		private String dataSource;
		private String query;
		private String id;
		private LinkedHashMap<String, DbFieldType> tableFields;
		private long createTime;
		private String resultTable;
		private CacheStatus cacheStatus;
		private long timeoutTime;
		private long capacity;
		private String isFullData;

		/**
		 * 创建， 检查在这里做
		 * 
		 * @return
		 */
		public CacheRecord build() {
			CacheRecord entry = new CacheRecord();
			entry.dataSource = this.dataSource;
			entry.query = this.query;
			entry.id = this.id;
			entry.tableFields = this.tableFields;
			entry.createTime = this.createTime;
			entry.resultTable = this.resultTable;
			entry.cacheStatus = this.cacheStatus;
			entry.timeoutTime = this.timeoutTime;
			entry.capacity = this.capacity;
			entry.isFullData = this.isFullData;
			
			return entry;
		}

		/**
		 * Setter
		 * 
		 * @param dataSource
		 * @return
		 */
		public CacheEntryBuilder dataSource(String dataSource) {
			this.dataSource = dataSource;
			return this;
		}

		/**
		 * Setter
		 * 
		 * @param query
		 * @return
		 */
		public CacheEntryBuilder query(String query) {
			this.query = query;
			return this;
		}

		/**
		 * Setter
		 * 
		 * @param id
		 * @return
		 */
		public CacheEntryBuilder id(String id) {
			this.id = id;
			return this;
		}

		/**
		 * Setter
		 * 
		 * @param tableFields
		 * @return
		 */
		public CacheEntryBuilder tableFields(LinkedHashMap<String, DbFieldType> tableFields) {
			this.tableFields = tableFields;
			return this;
		}

		/**
		 * Setter
		 * 
		 * @param createTime
		 * @return
		 */
		public CacheEntryBuilder createTime(long createTime) {
			this.createTime = createTime;
			return this;
		}

		/**
		 * Setter
		 * 
		 * @param resultTable
		 * @return
		 */
		public CacheEntryBuilder resultTable(String resultTable) {
			this.resultTable = resultTable;
			return this;
		}

		/**
		 * Setter
		 * 
		 * @param cacheStatus
		 * @return
		 */
		public CacheEntryBuilder cacheStatus(CacheStatus cacheStatus) {
			this.cacheStatus = cacheStatus;
			return this;
		}

		/**
		 * Setter
		 * 
		 * @param timeoutTime
		 * @return
		 */
		public CacheEntryBuilder timeoutTime(long timeoutTime) {
			this.timeoutTime = timeoutTime;
			return this;
		}

		/**
		 * Setter
		 * 
		 * @param capacity
		 * @return
		 */
		public CacheEntryBuilder capacity(long capacity) {
			this.capacity = capacity;
			return this;
		}
		
		public CacheEntryBuilder isFullData(String isFullData) {
			this.isFullData = isFullData;
			return this;
		}
	}

	/**
	 * 返回数据库表示， 例如： aField=VARCHAR,bField=DECIMAL
	 * @return
	 */
	public String getTableFieldsStr() {
		List<String> pairs = new ArrayList<>();
		for (Entry<String, DbFieldType> e : tableFields.entrySet()) {
			pairs.add(e.getKey() + "=" + e.getValue());
		}
		return Joiner.on(",").join(pairs);
	}
}
