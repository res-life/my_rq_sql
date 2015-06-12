package com.yeahmobi.datasystem.query.skeleton;

import io.druid.data.input.MapBasedRow;
import io.druid.query.Result;
import io.druid.query.groupby.orderby.OrderByColumnSpec;
import io.druid.query.groupby.orderby.OrderByColumnSpec.Direction;
import io.druid.query.timeseries.TimeseriesResultValue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.joda.time.DateTime;

import com.yeahmobi.datasystem.query.landingpage.H2InMemoryDbUtil;
import com.yeahmobi.datasystem.query.meta.ReportResult;
import com.yeahmobi.datasystem.query.meta.TimeFilter;
import com.yeahmobi.datasystem.query.process.QueryType;
import com.yeahmobi.datasystem.query.reportrequest.DataRange;
import com.yeahmobi.datasystem.query.timedimension.TimeDimension;

/**
 * 表示数据库存储druid 结果<br>
 * 结果已经被druid保存到数据库了<br>
 * 
 */
public class DataBaseDataSet extends AbstractDataSet {

	// 数据库的jdbc url
	private String url;
	private String tableName;
	private PostContext request;
	private Map<String, String> lastTableName = new HashMap<String, String>();

	/**
	 * 构造函数
	 * 
	 * @param request
	 * @param url
	 *            数据库的jdbc url
	 * @tablename table name
	 */
	public DataBaseDataSet(PostContext request, String url, String tablename) {
		this.request = request;
		this.url = url;
		this.tableName = tablename;
	}

	@Override
	public PostContext getRequest() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int size() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public List<Object> getAllData() {
		List<String> fields = new ArrayList<String>((request.getReportContext().isDoTimeDb() || request.isTimeSort())? request.getFieldTypeMap().get("dbGroupFieldTypeMap").keySet() : request.getFieldTypeMap().get("dbFieldTypeMap").keySet());
		Map<String, String> fieldTypeMap = (request.getReportContext().isDoTimeDb() || request.isTimeSort())? request.getFieldTypeMap().get("dbGroupFieldTypeMap") : request.getFieldTypeMap().get("dbFieldTypeMap");
		String lastTableName = getInfo().get("lastTableName");
		// 增加列数用于做时间字段的一些处理
		if(request.forLpTimeFilter()){
			alterLastTable(lastTableName);
			updateLastTable(lastTableName);
		}
		Map<String , TimeFilter> timeFilters = request.getParser().timeFilters;
		if(!timeFilters.isEmpty()) timeFilter(lastTableName , timeFilters);
		// int startIndex = request.getParser().size * request.getParser().page + 1;
		int startIndex = DataRange.getStart(request.getParser().page, request.getParser().size, request.getParser().offset) + 1;
		int size = request.getParser().size;
		List<OrderByColumnSpec> columns = new ArrayList<OrderByColumnSpec>(request.getParser().columns);
		columns.addAll(request.getParser().timeColumns);
		String columnsString = columns.size()>0? columnToString(columns): "";
		List<List<Object>> rows = H2InMemoryDbUtil.readData(lastTableName,
				columnsString,fields, startIndex, size, "wrong");
		List<Object> rs = new LinkedList<>();
		if(QueryType.TIMESERIES.compareTo(request.getQueryContext().getQueryType()) == 0){
			for (List<Object> row : rows) {
				DateTime timestamp = DateTime.now();
				Map<String, Object> event = new HashMap<String, Object>();
				int i = 0;
				for (Entry<String, String> entry : fieldTypeMap.entrySet()) {
					if (entry.getKey().equals("timestamp")) {
						timestamp = DateTime.parse((String) row.get(i));
					} else {
						event.put(entry.getKey(), row.get(i));
					}
					++i;
				}
				rs.add(new Result<TimeseriesResultValue>(timestamp,
						new TimeseriesResultValue(event)));
			}
			return rs;
		}else{
			for (List<Object> row : rows) {
				DateTime timestamp = DateTime.now();
				Map<String, Object> event = new HashMap<String, Object>();
				int i = 0;
				for (Entry<String, String> entry : fieldTypeMap.entrySet()) {
					if (entry.getKey().equals("timestamp")) {
						timestamp = DateTime.parse((String) row.get(i));
					} else {
						event.put(entry.getKey(), row.get(i));
					}
					++i;
				}

				rs.add(new MapBasedRow(timestamp, event));
			}
			return rs;
		}
	}

	@Override
	public List<Object> subList(int fromIndex, int toIndex) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<List<Object>> partition(int size) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<String> getHeader() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<String, String> getInfo() {
		return lastTableName;
	}

	public void setLastTableName(String lastTableName) {
		this.lastTableName.put("lastTableName", lastTableName);
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	private void timeFilter(String lastTableName , Map<String , TimeFilter> timeFilters){
		StringBuilder sbTime = new StringBuilder();
		for (Entry<String, TimeFilter> a : timeFilters.entrySet()) {
			sbTime.append("or");
			sbTime.append(" ");
			sbTime.append(a.getValue().dimension.toString());
			sbTime.append(a.getValue().dimensionToken.getToken());
			sbTime.append("'");
			sbTime.append(a.getValue().getTimeSelected());
			sbTime.append("'");
			sbTime.append(" ");
		}
		String sql = "delete from " + lastTableName + " where" + sbTime.substring(2);
		String errorMsg = "time filter failed";
		H2InMemoryDbUtil.executeDbStatement(sql, errorMsg);
	}
	
	private String columnToString(List<OrderByColumnSpec> columns){
		StringBuilder sbColumns = new StringBuilder();
		for (OrderByColumnSpec orderByColumnSpec : columns) {
			sbColumns.append(",");
			if(ArrayUtils.contains(Arrays.asList("year","hour").toArray(), orderByColumnSpec.getDimension())){
				sbColumns.append("cast(" + orderByColumnSpec.getDimension() + " as decimal)");
			}else{
				sbColumns.append(orderByColumnSpec.getDimension());
			}
			if(orderByColumnSpec.getDirection() == Direction.ASCENDING){
				sbColumns.append(" asc");
			}else if(orderByColumnSpec.getDirection() == Direction.DESCENDING){
				sbColumns.append(" desc");
			}
		}
		return " order by " + sbColumns.substring(1);
	}
	
	private void alterLastTable(String tableName){
		if(request.getParser().intervalUnits.size() != 0){
			List<String> timeDimensions = new ArrayList<String>();
			for(String string : request.getParser().intervalUnits){
				timeDimensions.add(string + " varchar");
			}
			String addColumn = StringUtils.join(timeDimensions, ",");
			
			String sql = "alter table " + tableName + " add (" + addColumn + ")";
			String errorMsg = String.format("create update failed, sql is "
					+ sql);
			H2InMemoryDbUtil.executeDbStatement(sql, errorMsg);
		}
	}
	
	private void updateLastTable(String tableName){
		
		if(request.getParser().intervalUnits.size() != 0){
			HashMap<String, String> timeMap = new HashMap<String, String>();
			timeMap.put("year", "year(cast(substr(timestamp,0,23) as timestamp))");
			timeMap.put("month", "casewhen(month(cast(substr(timestamp,0,23) as timestamp))<10,concat('0',month(cast(substr(timestamp,0,23) as timestamp))),cast(month(cast(substr(timestamp,0,23) as timestamp)) as char))");
			timeMap.put("week", "concat(casewhen(month(cast(substr(timestamp,0,23) as timestamp))<10,concat('0',month(cast(substr(timestamp,0,23) as timestamp))),cast(month(cast(substr(timestamp,0,23) as timestamp)) as char)),'-',weekofmonth(substr(timestamp,0,22)))");
			timeMap.put("day", "cast(cast(substr(timestamp,0,23) as timestamp) as date)");
			timeMap.put("hour", "hour(cast(substr(timestamp,0,23) as timestamp))");
			
			StringBuffer sb  = new StringBuffer();
			for(String string : request.getParser().intervalUnits){
				sb.append(",");
				sb.append(string);
				sb.append(" = ");
				sb.append(timeMap.get(string));
			}
			
			String sql = "update " + tableName + " set "+sb.substring(1) + "where 1 = 1";
			String errorMsg = String.format("create update failed, sql is "
					+ sql);
			H2InMemoryDbUtil.executeDbStatement(sql, errorMsg);
		}
	}
}
