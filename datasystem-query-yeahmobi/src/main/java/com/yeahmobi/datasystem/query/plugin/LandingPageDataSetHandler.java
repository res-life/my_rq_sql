package com.yeahmobi.datasystem.query.plugin;

import io.druid.query.aggregation.PostAggregator;
import io.druid.query.groupby.orderby.OrderByColumnSpec;
import io.druid.query.groupby.orderby.OrderByColumnSpec.Direction;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import com.yeahmobi.datasystem.query.landingpage.H2InMemoryDbUtil;
import com.yeahmobi.datasystem.query.meta.LandingPageCfg;
import com.yeahmobi.datasystem.query.skeleton.DataBaseDataSet;
import com.yeahmobi.datasystem.query.skeleton.DataSet;
import com.yeahmobi.datasystem.query.skeleton.DefaultDataSetHandler;
import com.yeahmobi.datasystem.query.skeleton.PostContext;

public class LandingPageDataSetHandler extends DefaultDataSetHandler{
	private PostContext request;
	
	public LandingPageDataSetHandler(PostContext request) {
		this.request = request;
	}

	@Override
	public DataSet processDataSet(DataSet dataSet) {
		DataSet ret = null;
		if("lp".equalsIgnoreCase(request.getReportParam().getSettings().getProcess_type())){
			// 时间维度处理
			ret = landingPageAgg(dataSet);
		}
		
		// 运行处理后续的责任链上的handler
		return super.processDataSet(ret);
	}

	/**
	 * TODO haiwei
	 * @param dataSet
	 */
	private DataSet landingPageAgg(DataSet dataSet) {
		DataBaseDataSet dataBaseDataSet = (DataBaseDataSet)dataSet;
		String timeTablename2 = dataBaseDataSet.getTableName();
		String groupTableName = "group_" + timeTablename2;
		String joinTableName = "join_" + timeTablename2;
//		joinTable(groupTableName,joinTableName);
		dropLandingPageTable(groupTableName);
		return dataSet;
	}
	
	private boolean joinTable(String groupTableName,String joinTableName) {

		try {
			logger.info("landing page begin to join table " + groupTableName);

			LinkedHashMap<String, String> metrics = LandingPageCfg
					.getInstance().getMetrics();

			List<String> postAggregators = new ArrayList<String>();
			for (PostAggregator postAggregator : request.getParser().postAggregators) {
				postAggregators.add(postAggregator.getName());
			}
			String joinSql = generateJoinSql(groupTableName, joinTableName,
					new ArrayList<String>(request.getParser().groupByDimensions.keySet()), new ArrayList<String>(request.getParser().aggregators.keySet()),
					postAggregators,request.getParser().intervalUnits, metrics, request.getParser().columns);

			String errorMsg = String.format("join table failed, sql is "
					+ joinSql);
			H2InMemoryDbUtil.executeDbStatement(joinSql, errorMsg);
			logger.debug("join table sql is: " + joinSql);
			logger.info("landing page end to join table " + groupTableName);
			return true;
		} catch (Exception e) {
			logger.error("", e);
			return false;
		}
	}
	
	private String generateJoinSql(String groupTableName, String groupJoinTableName,
			List<String> groupBys, List<String> aggregators,
			List<String> postAggregators, List<String> timeGroups ,
			LinkedHashMap<String, String> metrics,List<OrderByColumnSpec> orderByDimensions){
		   // generate fields, only click and cost is from a table
				List<String> fields = new ArrayList<String>();
				for (String f : timeGroups) {
					fields.add("a." + f);
				}
				for (String f : groupBys) {
					if (f.equals("offer_id")) {
						String str = "b." + f;
						String protectStr = " ifnull(" + str + ",'-1')";
						fields.add(protectStr);
					} else {
						fields.add("a." + f);
					}
				}
				for (String f : aggregators) {
					if ("clicks".equals(f) || "cost".equals(f)) {
						fields.add("a." + f + " as " + f);
					} else {
						String str = "b." + f;
						String protectStr = "ifnull(" + str + ",0)" + " as " + f;
						fields.add(protectStr);
					}
				}
				for (String f : postAggregators) {
					fields.add(metrics.get(f) + " as " + f);
				}
				String fieldsStr = StringUtils.join(fields, ",");

				// generate b table fields, should delete the clicks and cost
				List<String> bFields = new ArrayList<String>();
				bFields.addAll(timeGroups);
				bFields.addAll(groupBys);
				bFields.addAll(aggregators);
				bFields.addAll(postAggregators);
				bFields.remove("clicks");
				bFields.remove("cost");
				String bFieldStr = StringUtils.join(bFields, ",");

				// on statement, should use the group by fields and delete the offer_id
				List<String> tmpGroupbys = new ArrayList<String>(groupBys);
				tmpGroupbys.remove("offer_id");
				List<String> ons = new ArrayList<String>();
				for (String f : tmpGroupbys) {
					ons.add("a." + f + "=" + "b." + f);
				}
				for (String f : timeGroups) {
					ons.add("a." + f + "=" + "b." + f);
				}
				String onStr = StringUtils.join(ons, " and ");

				// generate a table fields, use all the on fields and clicks and cost
				List<String> aFields = new ArrayList<String>();
				aFields.addAll(timeGroups);
				aFields.addAll(tmpGroupbys);
				if (aggregators.contains("clicks")) {
					aFields.add("clicks");
				}
				if (aggregators.contains("cost")) {
					aFields.add("cost");
				}

				String aFieldStr = StringUtils.join(aFields, ",");

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
				
				String insertSql = String
						.format("insert into %s ( select %s from  (select %s from %s where offer_id = -1) a left join (select %s from %s where offer_id != -1) b on %s %s)",
								groupJoinTableName, fieldsStr, aFieldStr, groupTableName, bFieldStr,
								groupTableName, onStr, strOrderCol.toString());
				return insertSql;
	}
	
	private void dropLandingPageTable(String groupTablename) {

		String sql = "drop table if exists " + groupTablename;
		String errorMsg = "execute sql failed: " + sql;

		try {
			H2InMemoryDbUtil.executeDbStatement(sql, errorMsg);
		} catch (Exception e) {
			logger.error("", e);
		}
	}
	
	private static final Logger logger = Logger.getLogger(LandingPageDataSetHandler.class);

}
