package com.yeahmobi.datasystem.query.impala.plugin;

import io.druid.data.input.MapBasedRow;
import io.druid.query.Result;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.groupby.orderby.OrderByColumnSpec;
import io.druid.query.groupby.orderby.OrderByColumnSpec.Direction;
import io.druid.query.timeseries.TimeseriesResultValue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.rmrodrigues.pf4j.web.PluginManagerHolder;

import ro.fortsoft.pf4j.Extension;
import ro.fortsoft.pf4j.Plugin;
import ro.fortsoft.pf4j.PluginWrapper;

import com.google.common.base.Stopwatch;
import com.yeahmobi.datasystem.query.antlr4.DruidReportParser;
import com.yeahmobi.datasystem.query.exception.ReportRuntimeException;
import com.yeahmobi.datasystem.query.extensions.Impala;
import com.yeahmobi.datasystem.query.extensions.ImpalaJdbc;
import com.yeahmobi.datasystem.query.impala.SqlGenerator;
import com.yeahmobi.datasystem.query.meta.DimensionTable;
import com.yeahmobi.datasystem.query.meta.ImpalaCfg;
import com.yeahmobi.datasystem.query.meta.ImpalaCfgItem;
import com.yeahmobi.datasystem.query.meta.LandingPageCfg;
import com.yeahmobi.datasystem.query.meta.MetricAggTable;
import com.yeahmobi.datasystem.query.meta.TimeFilter;
import com.yeahmobi.datasystem.query.meta.ValueType;
import com.yeahmobi.datasystem.query.process.QueryType;
import com.yeahmobi.datasystem.query.skeleton.DataSourceViews;

/**
 * plugin of trading desk read file
 * 
 * @author ellis 2014.9.12
 */
public class ImpalaImp extends Plugin {

	private static Logger logger = Logger.getLogger(ImpalaImp.class);

	public ImpalaImp(PluginWrapper wrapper) {
		super(wrapper);
	}

	@Extension
	public static class UseImpalaImp implements Impala {

		@Override
		public List<Object> doImpalaHandle(String dataSource, DruidReportParser parser, QueryType queryType) {
			Stopwatch stopimpalawatch = Stopwatch.createStarted();
			// 判断是否要用impala进行处理，之后会由表路由做，目前由读取配置文件进行判断，目前条件为[subn,data为空的情况]
			LinkedHashMap<String, String> fieldTypeMap = new LinkedHashMap<String, String>();
			
			String impalaSql = null;
			if("contrack_druid_datasource_ds".equalsIgnoreCase(dataSource) || "eve_druid_datasource_ds".equalsIgnoreCase(dataSource)){
				impalaSql = impalaSqlForTd(dataSource, parser, fieldTypeMap);
			}else{
				impalaSql = impalaSql(dataSource, parser, fieldTypeMap);
			}

			if(logger.isDebugEnabled())
				logger.debug("[execute impala][sql is]--> " + impalaSql);
			try {

				// 调用插件， 访问数据库
				List<ImpalaJdbc> jdbcs = PluginManagerHolder.getPluginManager().getExtensions(ImpalaJdbc.class);
				List<List<Object>> rows = jdbcs.get(0).query(ImpalaCfg.getInstance().getDatasources().get(dataSource).getUseDatabase(), impalaSql);
				logger.debug("[execute impala sql time is]--> " + stopimpalawatch.elapsed(TimeUnit.MILLISECONDS));

				// 把结果封装成druid结果集，这样可以共用后处理
				List<Object> res = new LinkedList<>();
				if (QueryType.TIMESERIES.compareTo(queryType) == 0) {
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
						res.add(new Result<TimeseriesResultValue>(timestamp, new TimeseriesResultValue(event)));
					}
				} else {
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

						res.add(new MapBasedRow(timestamp, event));
					}
				}
				return res;
			} catch (Exception e) {
				logger.error("execute sql " + impalaSql + " error", e);
				logger.error(e.getClass(), e);
				throw new ReportRuntimeException(e, "failed to query Impala, sql is %s", impalaSql);
			}
		}
		
		private String impalaSql(String dataSource, DruidReportParser parser,
				LinkedHashMap<String, String> fieldTypeMap) {
			String[] typeIsTimeDimension = {"click_time","conv_time","expiration_date","time_stamp"};
			Map<String, Integer> typeIsTimeDimensionMaps = new HashMap<String, Integer>();
			
			List<OrderByColumnSpec> orderByDimensions = parser.columns;

			List<String> groupbys = new ArrayList<String>(parser.groupByDimensions.keySet());

			List<String> aggregators = new ArrayList<String>(parser.aggregators.keySet());

			List<String> postAggregators = new ArrayList<String>();
			for (PostAggregator postAggregator : parser.postAggregators) {
				postAggregators.add(postAggregator.getName());
			}

			List<String> timeDemsions = parser.intervalUnits;
			// 获取字段的类型
			DimensionTable dimensionTable = DataSourceViews.getViews().get(dataSource).dimentions().getDimensionTable();
			MetricAggTable metricAggTable = DataSourceViews.getViews().get(dataSource).metrics().getMetricAggTable();
			
			// group by 内容
			StringBuilder groupbyStr = new StringBuilder();
			
			if (groupbys.size() > 0) {
				groupbyStr.append(" group by ");
				groupbys.addAll(timeDemsions);
				groupbyStr.append(StringUtils.join(groupbys, ","));
			}
			groupbys.removeAll(timeDemsions);
			int index = 0;			
			for (String group : groupbys) {
				dimensionTable.getValueType(group);
				fieldTypeMap.put(group, dimensionTable.getValueType(group).toString());
				if(ArrayUtils.contains(typeIsTimeDimension, group)){
					typeIsTimeDimensionMaps.put(group, index);
				}
				index++;
			}
//			groupbys.removeAll(typeIsTimeDimensionMaps.keySet());
//			groupbys.addAll(typeIsTimeDimensionMaps.values()); "cast(" + group +" as string) as " + group
			for(String typeIsTimeDimensionMap: typeIsTimeDimensionMaps.keySet()){
				groupbys.remove(typeIsTimeDimensionMap);
				groupbys.add(typeIsTimeDimensionMaps.get(typeIsTimeDimensionMap), "cast(" + typeIsTimeDimensionMap +" as string) as " + typeIsTimeDimensionMap);
			}
			
			for (String aggregator : aggregators) {
				int precision = metricAggTable.getAggPrecision(aggregator);
				fieldTypeMap.put(aggregator, precision > 0 ? "DECIMAL" : "BIGINT");
			}
			for (String postAggregator : postAggregators) {
				int precision = metricAggTable.getAggPrecision(postAggregator);
				fieldTypeMap.put(postAggregator, precision > 0 ? "DECIMAL" : "BIGINT");
			}
			for (String timeDemsion : timeDemsions) {
				fieldTypeMap.put(timeDemsion, "STRING");
			}

			// 选择的维度
			StringBuilder selectDimensionStr = new StringBuilder();
			selectDimensionStr.append(StringUtils.join(groupbys, ","));
			// 选择的metic,先做公式替换
			LinkedHashMap<String, String> impalaMetrics = ImpalaCfg.getInstance().getDatasources().get(dataSource).getImpala();
			for (String aggregator : aggregators) {
				selectDimensionStr.append(",");
				selectDimensionStr.append(impalaMetrics.get(aggregator));
				selectDimensionStr.append(" as " + aggregator);
			}

			for (String postAggregator : postAggregators) {
				selectDimensionStr.append(",");
				selectDimensionStr.append(impalaMetrics.get(postAggregator));
				selectDimensionStr.append(" as " + postAggregator);
			}
			if(groupbys.size() < 1)
				selectDimensionStr = new StringBuilder(selectDimensionStr.toString().substring(1));
			
			// 获取时间维度的函数
			LinkedHashMap<String, String> timeChanges = ImpalaCfg.getInstance().getTimeFunction();
			for (String timeDemsion : timeDemsions) {
				selectDimensionStr.append(",");
				selectDimensionStr.append(timeChanges.get(timeDemsion).replaceAll("time_stamp", "seconds_add(cast(unix_timestamp(time_stamp) as timestamp), cast(" + parser.tzMinOffset + " * 60 as INT))"));
				selectDimensionStr.append(" as " + timeDemsion);
			}

			String havingStr = (parser.having == null) ? null : "having " + SqlGenerator.genHavingSql(parser.having);
			// 目前目标表新先写死，没用router
			String table = ImpalaCfg.getInstance().getDatasources().get(dataSource).getTargetTable();
			
			
			// .toString("yyyy-MM-dd HH:mm:ss")
			// 首末时间,时区问题这边要特殊处理一下 part
			DateTime startTime = parser.intervals.get(0).getStart();
			DateTime endTime = parser.intervals.get(0).getEnd();

			// 这边的过滤没有把时间的过滤包含进去，因为解析时就已经把时间的条件剔除出来了 parser.types
			Map<String, ValueType> types = new HashMap<String, ValueType>();
			Set<String> dimensionTables = dimensionTable.getTable().keySet();
			Set<String> metricAggTables = metricAggTable.getTable().keySet();
			for(String type: parser.types.keySet()){
				if(dimensionTables.contains(type)){
					types.put(type, dimensionTable.getValueType(type));
				}else if(metricAggTables.contains(type)){
					types.put(type, metricAggTable.getAggPrecision(type)> 0 ? ValueType.DECIMAL : ValueType.INTEGER);
				}else{
					types.put(type, ValueType.STRING);
				}
			}
			String filterStr = (parser.filter == null) ? "" : " and "
					+ SqlGenerator.genFilterSql(parser.filter, types);
			
			// 目前的支持的时间区间为7天，这边做了一个限制
			DateTime sevenDayLater = startTime.plusDays(7);
			endTime = sevenDayLater.isBefore(endTime)? sevenDayLater: endTime;
			
			String partFilter = "where (part1>='" + startTime.toString("yyyy-MM-dd") + "'" + " and part1<='"
					+ endTime.toString("yyyy-MM-dd") + "')";
			
			String timeFilter = String
					.format(" and (unix_timestamp(time_stamp) >= unix_timestamp('%s') and unix_timestamp(time_stamp) < unix_timestamp('%s'))",
							startTime.toString("yyyy-MM-dd HH:mm:ss"), endTime.toString("yyyy-MM-dd HH:mm:ss"));
			// 此处为时间过滤
			Map<String, TimeFilter> timeDimensions = parser.timeFilters;
			StringBuilder sbTime = new StringBuilder();
			for (Entry<String, TimeFilter> a : timeDimensions.entrySet()) {
				sbTime.append("and");
				sbTime.append(" ");
				if("hour".equalsIgnoreCase(a.getValue().dimension.toString())){
					sbTime.append(a.getValue().dimension.toString());
					sbTime.append(a.getValue().dimensionToken.convert());
					sbTime.append(a.getValue().getTimeSelected());
					sbTime.append(" ");
				}else{
					sbTime.append(a.getValue().dimension.toString());
					sbTime.append(a.getValue().dimensionToken.convert());
					sbTime.append("'");
					sbTime.append(a.getValue().getTimeSelected());
					sbTime.append("'");
					sbTime.append(" ");
				}
			}
			if(havingStr != null){
				havingStr += timeDimensions.size() > 0 ? " and (" + sbTime.substring(3) + ")" : "";
			}else{
				havingStr = timeDimensions.size() > 0 ? " having (" + sbTime.substring(3) + ")" : "";
			}

			// order by 条件
			StringBuffer strOrderCol = new StringBuffer();
			orderByDimensions.addAll(parser.timeColumns);
			if (orderByDimensions.size() > 0) {
				strOrderCol.append(" order by ");
				StringBuffer strOrderColDimens = new StringBuffer();
				for (OrderByColumnSpec ordercol : orderByDimensions) {
					strOrderColDimens.append(",");
					strOrderColDimens.append(ordercol.getDimension());
					if (ordercol.getDirection().compareTo(Direction.ASCENDING) == 0) {
						strOrderColDimens.append(" asc");
					} else {
						strOrderColDimens.append(" desc");
					}
				}
				strOrderCol.append(strOrderColDimens.substring(1));
			}

			// 分页 待定

			String impalaSql = String.format("select %s from %s %s %s %s %s limit %s", selectDimensionStr.toString(),
					table, partFilter + timeFilter + filterStr, groupbyStr.toString(), havingStr,
					strOrderCol.toString(), parser.maxRows);
			return impalaSql;
		}
		
		/*
		 * fieldTypeMap 这个map插入时有先后的顺序，需要和select的顺序保持一致
		 */
		private String impalaSqlForTd(String dataSource, DruidReportParser parser,
				LinkedHashMap<String, String> fieldTypeMap) {
			String[] typeIsTimeDimension = {"click_time","conv_time","expiration_date","time_stamp"};
			Map<String, Integer> typeIsTimeDimensionMaps = new HashMap<String, Integer>();
			
			List<OrderByColumnSpec> orderByDimensions = parser.columns;

			List<String> groupbys = new ArrayList<String>(parser.groupByDimensions.keySet());

			List<String> aggregators = new ArrayList<String>(parser.aggregators.keySet());

			List<String> postAggregators = new ArrayList<String>();
			for (PostAggregator postAggregator : parser.postAggregators) {
				postAggregators.add(postAggregator.getName());
			}

			List<String> timeDemsions = parser.intervalUnits;
			// 获取字段的类型
			DimensionTable dimensionTable = DataSourceViews.getViews().get(dataSource).dimentions().getDimensionTable();
			MetricAggTable metricAggTable = DataSourceViews.getViews().get(dataSource).metrics().getMetricAggTable();
			
			// group by 内容
			StringBuilder groupbyStr = new StringBuilder();
			
			if (groupbys.size() > 0) {
				groupbyStr.append(" group by ");
				groupbys.addAll(timeDemsions);
				groupbyStr.append(StringUtils.join(groupbys, ","));
			}
			groupbys.removeAll(timeDemsions);
			int index = 0;			
			for (String group : groupbys) {
				dimensionTable.getValueType(group);
				fieldTypeMap.put(group, dimensionTable.getValueType(group).toString());
				if(ArrayUtils.contains(typeIsTimeDimension, group)){
					typeIsTimeDimensionMaps.put(group, index);
				}
				index++;
			}
			for(String typeIsTimeDimensionMap: typeIsTimeDimensionMaps.keySet()){
				groupbys.remove(typeIsTimeDimensionMap);
				groupbys.add(typeIsTimeDimensionMaps.get(typeIsTimeDimensionMap), "cast(" + typeIsTimeDimensionMap +" as string) as " + typeIsTimeDimensionMap);
			}
			
			for (String aggregator : aggregators) {
				int precision = metricAggTable.getAggPrecision(aggregator);
				fieldTypeMap.put(aggregator, precision > 0 ? "DECIMAL" : "BIGINT");
			}
			
			for (String timeDemsion : timeDemsions) {
				fieldTypeMap.put(timeDemsion, "STRING");
			}
			
			for (String postAggregator : postAggregators) {
				int precision = metricAggTable.getAggPrecision(postAggregator);
				fieldTypeMap.put(postAggregator, precision > 0 ? "DECIMAL" : "BIGINT");
			}

			// 选择的维度
			StringBuilder selectDimensionStr = new StringBuilder();
			selectDimensionStr.append(StringUtils.join(groupbys, ","));
			// 选择的metic,先做公式替换
			LinkedHashMap<String, String> impalaMetrics = ImpalaCfg.getInstance().getDatasources().get(dataSource).getImpala();
			for (String aggregator : aggregators) {
				selectDimensionStr.append(",");
				selectDimensionStr.append(impalaMetrics.get(aggregator));
				selectDimensionStr.append(" as " + aggregator);
			}

			if(groupbys.size() < 1)
				selectDimensionStr = new StringBuilder(selectDimensionStr.toString().substring(1));
			
			// 获取时间维度的函数
			LinkedHashMap<String, String> timeChanges = ImpalaCfg.getInstance().getTimeFunction();
			for (String timeDemsion : timeDemsions) {
				selectDimensionStr.append(",");
				selectDimensionStr.append(timeChanges.get(timeDemsion).replaceAll("time_stamp", "seconds_add(cast(unix_timestamp(substr(time_stamp,1,19)) as timestamp), cast(" + parser.tzMinOffset + " * 60 as INT))"));
				selectDimensionStr.append(" as " + timeDemsion);
			}
            String selectDimensionStrForLp = selectDimensionStr.toString();
            
			for (String postAggregator : postAggregators) {
				selectDimensionStr.append(",");
				selectDimensionStr.append(impalaMetrics.get(postAggregator));
				selectDimensionStr.append(" as " + postAggregator);
			}
			
			String havingStr = (parser.having == null) ? null : "having " + SqlGenerator.genHavingSql(parser.having);
			// 目前目标表新先写死，没用router
			String table = ImpalaCfg.getInstance().getDatasources().get(dataSource).getTargetTable();
			
			
			// .toString("yyyy-MM-dd HH:mm:ss")
			// 首末时间,时区问题这边要特殊处理一下 part
			DateTime startTime = parser.intervals.get(0).getStart();
			DateTime endTime = parser.intervals.get(0).getEnd();

			// 这边的过滤没有把时间的过滤包含进去，因为解析时就已经把时间的条件剔除出来了 parser.types
			Map<String, ValueType> types = new HashMap<String, ValueType>();
			Set<String> dimensionTables = dimensionTable.getTable().keySet();
			Set<String> metricAggTables = metricAggTable.getTable().keySet();
			for(String type: parser.types.keySet()){
				if(dimensionTables.contains(type)){
					types.put(type, dimensionTable.getValueType(type));
				}else if(metricAggTables.contains(type)){
					types.put(type, metricAggTable.getAggPrecision(type)> 0 ? ValueType.DECIMAL : ValueType.INTEGER);
				}else{
					types.put(type, ValueType.STRING);
				}
			}
			String filterStr = (parser.filter == null) ? "" : " and "
					+ SqlGenerator.genFilterSql(parser.filter, types);
			
			// 目前的支持的时间区间为7天，这边做了一个限制
			/*DateTime sevenDayAgo = null;
			if(!endTime.isAfterNow()){
				sevenDayAgo = endTime.plusDays(7);
				startTime = sevenDayAgo.isAfter(startTime)? sevenDayAgo: startTime;
			}else{
				endTime = DateTime.now(DateTimeZone.UTC);
				sevenDayAgo = endTime.plusDays(7);
				startTime = sevenDayAgo.isAfter(startTime)? sevenDayAgo: startTime;
			}*/
			DateTime sevenDayLater = startTime.plusDays(7);
			endTime = sevenDayLater.isBefore(endTime)? sevenDayLater: endTime;
			
			String partFilter = "where (part1>='" + startTime.toString("yyyy-MM-dd") + "'" + " and part1<='"
					+ endTime.toString("yyyy-MM-dd") + "')";
			
			String timeFilter = String
					.format(" and (unix_timestamp(substr(time_stamp,1,19)) >= unix_timestamp('%s') and unix_timestamp(substr(time_stamp,1,19)) < unix_timestamp('%s'))",
							startTime.toString("yyyy-MM-dd HH:mm:ss"), endTime.toString("yyyy-MM-dd HH:mm:ss"));
			// 此处为时间过滤
			Map<String, TimeFilter> timeDimensions = parser.timeFilters;
			StringBuilder sbTime = new StringBuilder();
			for (Entry<String, TimeFilter> a : timeDimensions.entrySet()) {
				sbTime.append("and");
				sbTime.append(" ");
				if("hour".equalsIgnoreCase(a.getValue().dimension.toString())){
					sbTime.append(a.getValue().dimension.toString());
					sbTime.append(a.getValue().dimensionToken.convert());
					sbTime.append(a.getValue().getTimeSelected());
					sbTime.append(" ");
				}else{
					sbTime.append(a.getValue().dimension.toString());
					sbTime.append(a.getValue().dimensionToken.convert());
					sbTime.append("'");
					sbTime.append(a.getValue().getTimeSelected());
					sbTime.append("'");
					sbTime.append(" ");
				}
			}
			if(havingStr != null){
				havingStr += timeDimensions.size() > 0 ? " and (" + sbTime.substring(3) + ")" : "";
			}else{
				havingStr = timeDimensions.size() > 0 ? " having (" + sbTime.substring(3) + ")" : "";
			}

			// order by 条件
			StringBuffer strOrderCol = new StringBuffer();
			orderByDimensions.addAll(parser.timeColumns);
			if (orderByDimensions.size() > 0) {
				strOrderCol.append(" order by ");
				StringBuffer strOrderColDimens = new StringBuffer();
				for (OrderByColumnSpec ordercol : orderByDimensions) {
					strOrderColDimens.append(",");
					strOrderColDimens.append(ordercol.getDimension());
					if (ordercol.getDirection().compareTo(Direction.ASCENDING) == 0) {
						strOrderColDimens.append(" asc");
					} else {
						strOrderColDimens.append(" desc");
					}
				}
				strOrderCol.append(strOrderColDimens.substring(1));
			}

			// 分页 待定

			String impalaSql = null;

			if(parser.processType != null){
				if(parser.processType.contains("lp")){
					String lpTargetTable = String.format("(select %s from %s %s %s %s)", selectDimensionStrForLp,
							table, partFilter + timeFilter + filterStr, groupbyStr.toString(), havingStr);
					
					impalaSql = generateJoinSql(lpTargetTable,parser) + " " +strOrderCol.toString() + " limit " + parser.maxRows;
					return impalaSql;
				}
			}
			
			impalaSql = String.format("select %s from %s %s %s %s %s limit %s", selectDimensionStr.toString(),
					table, partFilter + timeFilter + filterStr, groupbyStr.toString(), havingStr,
					strOrderCol.toString(), parser.maxRows);
			
			return impalaSql;
		}
		
		private String generateJoinSql(String lpTargetTable, DruidReportParser parser){
			
			List<String> groupbys = new ArrayList<String>(parser.groupByDimensions.keySet());

			List<String> aggregators = new ArrayList<String>(parser.aggregators.keySet());

			List<String> postAggregators = new ArrayList<String>();
			for (PostAggregator postAggregator : parser.postAggregators) {
				postAggregators.add(postAggregator.getName());
			}

			List<String> timeDemsions = parser.intervalUnits;
		    // generate fields, only click and cost is from a table
			List<String> fields = new ArrayList<String>();
			
			for (String f : timeDemsions) {
				fields.add("a." + f + " as " + f);
			}
			
			for (String f : groupbys) {
				if (f.equals("offer_id")) {
					String str = "b." + f;
					String protectStr = " ifnull(" + str + ",'-1')" + " as " + f;
					fields.add(protectStr);
				} else {
					fields.add("a." + f + " as " + f);
				}
			}
			for (String f : aggregators) {
				if ("clicks".equals(f) || "cost".equals(f)) {
					fields.add("a." + f + " as " + f);
				} else {
					String str = "b." + f;
					String protectStr =  "ifnull(" + str + ",0)" + " as " + f;
					fields.add(protectStr);
				}
			}
			
			LinkedHashMap<String, String> metrics = LandingPageCfg
					.getInstance().getMetrics();
			for (String f : postAggregators) {
				fields.add(metrics.get(f).replaceAll("decimal", "double") + " as " + f);
			}
			String fieldsStr = StringUtils.join(fields, ",");

			// generate b table fields, should delete the clicks and cost
			List<String> bFields = new ArrayList<String>();
			bFields.addAll(timeDemsions);
			bFields.addAll(groupbys);
			bFields.addAll(aggregators);
			bFields.remove("clicks");
			bFields.remove("cost");
			StringBuilder bFieldStr = new StringBuilder();
			for (String bField : bFields) {
				bFieldStr.append(",");
				bFieldStr.append("table2.");
				bFieldStr.append(bField);
				bFieldStr.append(" as ");
				bFieldStr.append(bField);
			}

			// on statement, should use the group by fields and delete the offer_id
			List<String> tmpGroupbys = new ArrayList<String>(groupbys);
			tmpGroupbys.remove("offer_id");
			List<String> ons = new ArrayList<String>();
			for (String f : tmpGroupbys) {
				ons.add("a." + f + "=" + "b." + f);
			}
			for (String f : timeDemsions) {
				ons.add("a." + f + "=" + "b." + f);
			}
			String onStr = StringUtils.join(ons, " and ");

			// generate a table fields, use all the on fields and clicks and cost
			List<String> aFields = new ArrayList<String>();
			
			aFields.addAll(timeDemsions);
			aFields.addAll(tmpGroupbys);
			if (aggregators.contains("clicks")) {
				aFields.add("clicks");
			}
			if (aggregators.contains("cost")) {
				aFields.add("cost");
			}

			StringBuilder aFieldStr = new StringBuilder();
			for (String aField : aFields) {
				aFieldStr.append(",");
				aFieldStr.append("table1.");
				aFieldStr.append(aField);
				aFieldStr.append(" as ");
				aFieldStr.append(aField);
			}

			String tdLpSql = String
					.format("select %s from  (select %s from %s as table1 where offer_id = '-1') a left join (select %s from %s as table2 where offer_id != '-1') b on %s",
							 fieldsStr, aFieldStr.substring(1) , lpTargetTable, bFieldStr.substring(1),
							 lpTargetTable, onStr);
			return tdLpSql;
		}
	}
	
}
