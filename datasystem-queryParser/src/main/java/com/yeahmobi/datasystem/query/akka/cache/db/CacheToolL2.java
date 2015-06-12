package com.yeahmobi.datasystem.query.akka.cache.db;

import io.druid.query.aggregation.PostAggregator;
import io.druid.query.groupby.orderby.OrderByColumnSpec;
import io.druid.query.groupby.orderby.OrderByColumnSpec.Direction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.yeahmobi.datasystem.query.akka.cache.CacheTool;
import com.yeahmobi.datasystem.query.akka.cache.Ttls;
import com.yeahmobi.datasystem.query.antlr4.DruidReportParser;
import com.yeahmobi.datasystem.query.antlr4.WarpInterpreter;
import com.yeahmobi.datasystem.query.exception.FileErrorListener;
import com.yeahmobi.datasystem.query.exception.ReportParserException;
import com.yeahmobi.datasystem.query.jersey.ReportServiceResult;
import com.yeahmobi.datasystem.query.landingpage.H2InMemoryDbUtil;
import com.yeahmobi.datasystem.query.meta.DimensionTable;
import com.yeahmobi.datasystem.query.meta.MetricAggTable;
import com.yeahmobi.datasystem.query.meta.MsgType;
import com.yeahmobi.datasystem.query.meta.ReportPage;
import com.yeahmobi.datasystem.query.meta.ReportResult;
import com.yeahmobi.datasystem.query.meta.ReportResult.Entity;
import com.yeahmobi.datasystem.query.meta.ValueType;
import com.yeahmobi.datasystem.query.reportrequest.DataRange;
import com.yeahmobi.datasystem.query.reportrequest.ReportParam;
import com.yeahmobi.datasystem.query.reportrequest.ReportParamFactory;
import com.yeahmobi.datasystem.query.skeleton.DataSourceViews;
import com.yeahmobi.datasystem.query.skeleton.PostContext;
import com.yeahmobi.datasystem.query.utils.DbUtils;
import com.yeahmobi.datasystem.query.utils.Four;

/**
 * Created by oscar 2014-09-23<br>
 * Level 2 cache, 保存在数据库中<br>
 * 
 */
public class CacheToolL2 implements CacheTool {

	// 以下是输入参数
	public String dataSource;

	private List<String> groupBys;

	private List<String> aggregators;

	private List<String> postAggregators;

	private List<String> timeDimentions;

	private List<OrderByColumnSpec> orderByDimensions;

	private boolean alreadySaveToDb;

	private int pageNumber;

	private int pageSize;

	// 以下是生成的变量
	private LinkedHashMap<String, DbFieldType> fieldTypeMap;

	private List<String> fieldsWithPresion;

	private List<String> fields;

	// 生成的惟一的id, 在表中时tableName字段
	private String rowId;

	private String resultTableName;

	private CacheLogic cacheLogic;

	private List<Integer> lengths;

	private boolean isImpala;

	/**
	 * 构造函数
	 * 
	 * @param dataSource
	 * @param groupBys
	 * @param aggregators
	 * @param postAggregators
	 * @param timeDimentions
	 * @param orderByDimensions
	 * @param alreadySaveToDb
	 */
	public CacheToolL2(String dataSource, List<String> groupBys, List<String> aggregators,
			List<String> postAggregators, List<String> timeDimentions, List<OrderByColumnSpec> orderByDimensions,
			boolean alreadySaveToDb, int pageNumber, int pageSize, boolean isImpala) {
		this.dataSource = dataSource;
		this.groupBys = groupBys;
		this.aggregators = aggregators;
		this.postAggregators = postAggregators;
		this.timeDimentions = timeDimentions;
		this.orderByDimensions = orderByDimensions;
		this.alreadySaveToDb = alreadySaveToDb;
		this.pageNumber = pageNumber;
		this.pageSize = pageSize;
		this.isImpala = isImpala;
	}

	@Override
	public boolean set(Object... args) {
		long start = System.currentTimeMillis();
		try {
			boolean ret = setImp(args);
			long end = System.currentTimeMillis();
			logger.info("set level 2 cache, used time ms: " + (end - start));
			return ret;
		} catch (Exception e) {
			logger.warn("save to cache level 2 error", e);
			return false;
		}
	}
	
	private boolean setImp(Object... args) {

		// 生成
		init();

		ReportParam request = (ReportParam) args[0];
		ReportResult reportResult = (ReportResult) args[1];
		boolean isFullData = (boolean) args[2];

		if(!DataRange.isFromBegin(request.getSettings().getPagination().getPage(), request.getSettings().getPagination().getOffset())){
			logger.info("page number or offset is not 0, so not save cache leve 2, report id is " + request.getSettings().getReport_id());
			return false;
		}

		String key = isFullData ? request.toStringForL2WithoutSort() : request.toStringForL2WithSort();
		
		long timeoutTime = Ttls.Dynamic.apply(request) * 1000;
		if(timeoutTime < 10 * 60 * 1000){
			// 如果缓存时间小于10分钟， 则不适用L2 cache
			logger.info("real time query not use cache level 2, key is " + key);
			return true;
		}

		ReportServiceResult ret = new ReportServiceResult(MsgType.success, reportResult);

		if (!isImpala && alreadySaveToDb) {

			// 已经处理过了， 不用再插入
			logger.warn("cache L2 logic error, should not be invoked here" + key);
			return true;
		}

		if(reportResult.getData().getData().size() <= 1){
			// 如果druid处理失败， 返回的size 是0， 不应该cache这种无效的值
			logger.info("cache level 2, data size is 0, not cache it." + key);
			return true;
		}
		
		// 对于二级缓存， 当size 大于10000时， 不缓存
		if (reportResult.getData().getData().size() > 10001) {
			logger.info("result is max than 10000 rows, so not save to L2 cache");
			return false;
		}

		// 创建元信息表
		// CacheRecord.createMetaTableIfNotExists();

		createCacheTable();

		insertToTable(resultTableName, fieldTypeMap, ret);

		// 在LP_META表中增加一行
		CacheRecord cacheEntry = genCacheEntry(request, timeoutTime, isFullData);

		// 如果容量不够， 会删除老的 READY 的cache
		cacheLogic.add(cacheEntry);

		logger.info(String.format("save to cache level 2 success, key is %s, is full data %s", key, isFullData));

		return true;
	}

	/**
	 * TODO 判断是否在范围内， 只判断post aggregator
	 */
	public Object get(Object obj, Class<?> clazz) {
		long start = System.currentTimeMillis();
		Object ret = null;
		try {
			ret = get(obj);
		} catch (Exception e) {
			logger.warn("get cache from level 2 cache failed", e);
			return null;
		}

		long end = System.currentTimeMillis();
		logger.info("get level 2 cache used time ms: " + (end - start));
		return ret;

	}

	/**
	 * 
	 * 1）impala, 直接读取， 因为存的是最终结果 2）同步， 就是插入数据库这种方式。 需要转换。 3）异步， 不需要转换，
	 * 因为保存的是最终结果
	 * 
	 * @param obj
	 * @return
	 */
	private Object get(Object obj) {
		ReportParam request = (ReportParam) obj;
		String keyWithSort = request.toStringForL2WithSort();
		keyWithSort = escapeSingleQuotesForSql(keyWithSort);

		String filter = String.format("query = '%s' and cachestatus = 'READY'", keyWithSort);
		List<List<Object>> ret = H2InMemoryDbUtil.selectTable(CacheRecord.getMetaTableName(),
				Arrays.asList("resulttable", "isfulldata"), filter);
		String resultTableName = ret.size() > 0 ? (String)(ret.get(0).get(0)) : null;
		String isFullDataStr = ret.size() > 0 ? (String)(ret.get(0).get(1)) : null;
		boolean isFullData = "true".equals(isFullDataStr) ? true : false;

		if(resultTableName != null){
			return getCache(request, resultTableName, isFullData);
		}else{
			
			String keyWithoutSort = request.toStringForL2WithoutSort();
			keyWithoutSort = escapeSingleQuotesForSql(keyWithoutSort);

			filter = String.format("query = '%s' and cachestatus = 'READY'", keyWithoutSort);
			ret = H2InMemoryDbUtil.selectTable(CacheRecord.getMetaTableName(),
					Arrays.asList("resulttable", "isfulldata"), filter);
			resultTableName = ret.size() > 0 ? (String)(ret.get(0).get(0)) : null;
			isFullDataStr = ret.size() > 0 ? (String)(ret.get(0).get(1)) : null;
			isFullData = "true".equals(isFullDataStr) ? true : false;
			if(null != resultTableName){
				return getCache(request, resultTableName, isFullData);
			}else{
				return null;
			}
			
		}
	}
	
	/**
	 * 从部分数据中找
	 * @param request
	 * @param resultTableName
	 * @param isFullData
	 * @return
	 */
	private Object getCache(ReportParam request, String resultTableName, boolean isFullData) {
		int pageNumber = request.getSettings().getPagination().getPage();
		int pageSize = request.getSettings().getPagination().getSize();
		int offset = request.getSettings().getPagination().getOffset();
		
		int totalRow = 0;
		try {
			totalRow = H2InMemoryDbUtil.tableRowSize(resultTableName, "");
		} catch (Exception e) {
			logger.error("read cache failed when get size", e);
			throw e;
		}

		int startIndex = DataRange.getStart(pageNumber, pageSize, offset) + 1;
		if(!isFullData){
			// 第一次查询cache了1000行数据。但是第二次查询是1001行数据， 说明第一次查询没有cache这么多数据, 不能使用这个cache
			// 如果缓存中的数据是[0 - N], 但是想查找 >N 的记录， 应该返回null， 不能使用cache; DB index start with 1
			int maxIndex = startIndex + pageSize;
			if(maxIndex > totalRow){
				return null;
			}			
		}
		
		return getCacheImp(request, resultTableName, startIndex, totalRow);
	}

	private Object getCacheImp(ReportParam request, String resultTableName, int startIndex, int totalRow) {
		List<List<Object>> data = null;

		// 这里已经公式替换了。
		String queryParams = ReportParamFactory.toString(request);

		// 语法解析
		DruidReportParser parser = null;
		try {
			String dataSource = request.getSettings().getData_source();
			parser = WarpInterpreter.convert(queryParams, dataSource, new FileErrorListener());
		} catch (ReportParserException e) {
			logger.error("parse failed when find level 2 cache", e);
			return null;
		}

		// 收集parser 信息
		List<OrderByColumnSpec> orderByDimensions = Lists.newArrayList(Iterables.concat(parser.columns,
				parser.timeColumns));
		Set<String> groupbys = parser.groupByDimensions.keySet();
		// 更改查询字段
		Set<String> aggregators = new HashSet<String>();
		if(!parser.aggregators.keySet().isEmpty()){
			for(String aggregator : parser.aggregators.keySet()){
				if(parser.fields.contains(aggregator))
					aggregators.add(aggregator);
			}
		}
		
		List<String> postAggregators = new ArrayList<String>();
		for (PostAggregator postAggregator : parser.postAggregators) {
			postAggregators.add(postAggregator.getName());
		}
		List<String> timeDemsions = parser.intervalUnits;
		
		// order by
		List<String> orders = Lists.transform(orderByDimensions, new Function<OrderByColumnSpec, String>() {
			@Override
			public String apply(OrderByColumnSpec input) {
				String dimentionStr = null;
				if (input.getDimension().equals("hour")) {
					dimentionStr = "cast(hour as int)";
				} else {
					dimentionStr = input.getDimension();
				}

				if (Direction.ASCENDING == input.getDirection()) {
					return dimentionStr + " " + "asc";
				} else {
					return dimentionStr + " " + "desc";
				}
			}
		});
		String ordersStr = Joiner.on(",").join(orders);
		
		// fields
		List<String> fields = Lists
				.newArrayList(Iterables.concat(timeDemsions, groupbys, aggregators, postAggregators));
		
		try {
			data = H2InMemoryDbUtil.readDataWithSrotWithPage(resultTableName, fields, startIndex,
					pageSize, ordersStr, "");
		} catch (Exception e) {
			logger.error("read cache failed when get data", e);
			throw e;
		}

		ReportResult result = genReportResult(fields, data, totalRow);

		// 增加cache生命周期: 10分钟
		int deltaTime = 10 * 60 * 1000;
		String sql = String.format("update %s set timeouttime = TIMEOUTTIME + %s where RESULTTABLE = '%s'",
				CacheRecord.getMetaTableName(), deltaTime, resultTableName);
		H2InMemoryDbUtil.executeDbStatement(sql, "");

		logger.info("get from cache level 2 success, key is " + request.toStringForL2WithSort());

		return result;
	}

	private String escapeSingleQuotesForSql(String key) {
		return key.replaceAll("'", "''");
	}

	private void init() {
		Four<LinkedHashMap<String, DbFieldType>, List<String>, List<String>, List<Integer>> info = initDbFieldMap(
				dataSource, timeDimentions, groupBys, aggregators, postAggregators);

		fieldTypeMap = info.t1;
		fieldsWithPresion = info.t2;
		fields = info.t3;
		lengths = info.t4;

		rowId = DbUtils.createUniqueTableName();
		resultTableName = DbUtils.createUniqueTableName();
		cacheLogic = CacheLogicFactory.newInstance(timeDimentions);
	}

	/**
	 * 工厂方法
	 * 
	 * @param postContext
	 * @return
	 */
	public static CacheToolL2 newInstance(PostContext postContext, boolean isImpala) {
		String dataSource = postContext.getParser().getDataSource();
		List<String> groupBys = new ArrayList<String>(postContext.getParser().groupByDimensions.keySet());
		List<String> aggregators = new ArrayList<String>(postContext.getParser().aggregators.keySet());
		List<String> postAggregators = Lists.transform(postContext.getParser().postAggregators,
				new Function<PostAggregator, String>() {
					@Override
					public String apply(PostAggregator input) {
						return input.getName();
					}
				});
		List<String> timeDimentions = postContext.getParser().intervalUnits;
		List<OrderByColumnSpec> orderByDimensions = postContext.getParser().columns;
		boolean alreadySaveToDb = postContext.isInsertDb();
		int pageNumber = postContext.getReportParam().getSettings().getPagination().getPage();
		int pageSize = postContext.getReportParam().getSettings().getPagination().getSize();
		return new CacheToolL2(dataSource, groupBys, aggregators, postAggregators, timeDimentions, orderByDimensions,
				alreadySaveToDb, pageNumber, pageSize, isImpala);
	}

	private static Four<LinkedHashMap<String, DbFieldType>, List<String>, List<String>, List<Integer>> initDbFieldMap(
			String dataSource, List<String> timeDimentions, List<String> groupBys, List<String> aggregators,
			List<String> postAggregators) {
		LinkedHashMap<String, DbFieldType> typeMap = new LinkedHashMap<>();
		List<String> detailList = new ArrayList<>();
		List<String> fields = new ArrayList<>();
		List<Integer> lengths = new ArrayList<>();

		MetricAggTable metricAggTable = DataSourceViews.getViews().get(dataSource).metrics().getMetricAggTable();
		DimensionTable dimensionTable = DataSourceViews.getViews().get(dataSource).dimentions().getDimensionTable();

		DbFieldType dbType = null;
		String dbTypeDetail = null;

		for (String time : timeDimentions) {
			dbType = DbFieldType.VARCHAR;
			dbTypeDetail = String.format("%s %s(%s)", time, dbType, 30);

			typeMap.put(time, dbType);
			detailList.add(dbTypeDetail);
			fields.add(time);
			lengths.add(30);
		}

		// handle group
		for (String group : groupBys) {
			ValueType type = dimensionTable.getValueType(group);
			if (ValueType.NUMBER == type || ValueType.INTEGER == type) {
				dbType = DbFieldType.BIGINT;
				dbTypeDetail = String.format("%s %s", group, dbType);
				lengths.add(8);
			} else if (ValueType.STRING == type) {
				dbType = DbFieldType.VARCHAR;
				int maxLength = dimensionTable.getTable().get(group).getMaxLength();
				dbTypeDetail = String.format("%s %s(%s)", group, dbType, maxLength);
				lengths.add(maxLength);
			} else if (ValueType.DECIMAL == type) {
				dbType = DbFieldType.DECIMAL;
				dbTypeDetail = String.format("%s %s(%s,%s)", group, dbType, 50, 3);
				lengths.add(8);
			} else {
				String msg = String.format("the type of %s is unkonw, please configure", group);
				throw new RuntimeException(msg);
			}

			typeMap.put(group, dbType);
			detailList.add(dbTypeDetail);
			fields.add(group);
		}

		// handle the aggregators
		for (String aggregator : aggregators) {
			int precision = metricAggTable.getAggPrecision(aggregator);
			if (precision > 0) {
				dbType = DbFieldType.DECIMAL;
				dbTypeDetail = String.format("%s %s(30,%s)", aggregator, dbType, precision);
			} else {
				dbType = DbFieldType.BIGINT;
				dbTypeDetail = String.format("%s %s", aggregator, dbType);
			}

			typeMap.put(aggregator, dbType);
			detailList.add(dbTypeDetail);
			fields.add(aggregator);
			lengths.add(8);
		}

		// handle the post aggregators
		for (String postAggregator : postAggregators) {
			int precision = metricAggTable.getAggPrecision(postAggregator);
			if (precision > 0) {
				dbType = DbFieldType.DECIMAL;
				dbTypeDetail = String.format("%s %s(30,%s)", postAggregator, dbType, precision);
			} else {
				dbType = DbFieldType.BIGINT;
				dbTypeDetail = String.format("%s %s", postAggregator, dbType);
			}

			typeMap.put(postAggregator, dbType);
			detailList.add(dbTypeDetail);
			fields.add(postAggregator);
			lengths.add(8);
		}

		return Four.of(typeMap, detailList, fields, lengths);
	}

	private ReportResult genReportResult(List<String> fields, List<List<Object>> dataList, int totalPageNumber) {

		ReportResult result = new ReportResult();
		Entity data = new Entity();
		List<Object[]> list = Lists.transform(dataList, new Function<List<Object>, Object[]>() {
			@Override
			public Object[] apply(List<Object> input) {
				return input.toArray();
			}
		});

		// 添加表头
		List<Object[]> finalResultList = new LinkedList<>();
		finalResultList.add(fields.toArray());

		List<Object[]> transedList = Lists.transform(list, new MonthAndWeekTransformer(fields));

		// 添加数据
		finalResultList.addAll(transedList);

		data.setData(finalResultList);
		ReportPage page = new ReportPage(pageNumber, totalPageNumber);
		data.setPage(page);
		result.setData(data);
		result.setMsg("ok");
		result.setFlag("success");
		return result;
	}

	private CacheRecord genCacheEntry(ReportParam request, long timeoutTime, boolean isFullData) {
		
		String query = null;
		if(isFullData){
			query = request.toStringForL2WithoutSort();
		}else{
			query = request.toStringForL2WithSort();
		}

		long createTime = System.currentTimeMillis();
		long timeAt = createTime + timeoutTime;

		CacheRecord record = CacheRecord.builder().dataSource(dataSource).query(query).id(rowId)
				.tableFields(fieldTypeMap).createTime(createTime).resultTable(resultTableName)
				.cacheStatus(CacheStatus.READY).timeoutTime(timeAt).capacity(0).build();

		// 查询capacity
		long capacity = calTableRowCol(record);
		record.setCapacity(capacity);
		String isFullDataStr = isFullData ? "true" : "false";
		record.setIsFullData(isFullDataStr);
		return record;
	}

	private void insertToTable(String tableName, LinkedHashMap<String, DbFieldType> fieldTypes, ReportServiceResult ret) {
		try {
			ReportResult result = (ReportResult) (ret.getResult());
			List<Object[]> data = result.getData().getData();

			Object[] head = data.get(0);
			List<Object[]> subData = data.subList(1, data.size());

			// trunck by the max length for String type
			subData = trunck(subData, fieldTypes, lengths);

			LinkedHashMap<String, DbFieldType> newfieldTypeds = new LinkedHashMap<>();
			for (Object obj : head) {
				newfieldTypeds.put((String) obj, fieldTypes.get(obj));
			}

			H2InMemoryDbUtil.insertData(tableName, newfieldTypeds, subData);
		} catch (Exception e) {
			logger.error("insert into table failed " + tableName, e);
		}
	}

	private List<Object[]> trunck(List<Object[]> subData, LinkedHashMap<String, DbFieldType> fieldTypes,
			List<Integer> lengths) {
		
		List<DbFieldType> types = new ArrayList<>(fieldTypes.values());
		
		List<Object[]> newData = new LinkedList<>();
		for (Object[] row : subData) {
			Object[] newRow = new Object[row.length];
			for(int i = 0; i < row.length; ++i){
				newRow[i] = trunck(row[i], types.get(i), lengths.get(i));
			}
			
			newData.add(newRow);
		}

		return newData;
	}

	private Object trunck(Object object, DbFieldType dbFieldType, Integer length) {
		
		if(DbFieldType.VARCHAR == dbFieldType){
			if(null == object){
				return null;
			}else{
				if(object.toString().length() > length){
					return object.toString().substring(0, length);
				}else{
					return object;
				}
			}
		}else{
			return object;
		}
	}

	private void createCacheTable() {
		String body = Joiner.on(',').join(fieldsWithPresion);
		String sql = String.format("create table %s (%s)", resultTableName, body);
		try {
			H2InMemoryDbUtil.executeDbStatement(sql, "");
		} catch (Exception e) {
			logger.error("create cache table failed", e);
		}
	}

	private static long calTableRowCol(CacheRecord cacheEntry) {
		try {
			String resultTableName = cacheEntry.getResultTable();

			int row = H2InMemoryDbUtil.tableRowSize(resultTableName, "");
			int col = cacheEntry.getTableFields().size();

			return (long) row * (long) col;
		} catch (Exception e) {
			logger.error("cout row size failed", e);
			return 0;
		}
	}

	private final static Logger logger = Logger.getLogger(CacheToolL2.class);
}
