package com.yeahmobi.datasystem.query.process;

/**
 * Created by yangxu on 3/24/14.
 */

import io.druid.data.input.MapBasedRow;
import io.druid.data.input.Row;
import io.druid.query.Druids;
import io.druid.query.Result;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.groupby.GroupByQuery;
import io.druid.query.timeseries.TimeseriesResultValue;
import io.druid.query.topn.TopNQueryBuilder;
import io.druid.query.topn.TopNResultValue;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.fasterxml.jackson.core.type.TypeReference;
import com.yeahmobi.datasystem.query.antlr4.DruidReportParser;

public class QueryFactory {

	private static Logger logger = Logger.getLogger(QueryFactory.class);

	/**
	 * druid的请求类型目前共有6个：
	 * <p>
	 * groupby、search、segmentMetadata、timeBoundary、timeseries、topN
	 * <p>
	 * 目前实现了timeseries、groupby
	 * 
	 * @param parser
	 * @return
	 */
	public static QueryContext create(DruidReportParser parser, String queryParam) {
		if (null == parser)
			return null;

		QueryContext queryContext = new QueryContext();
		// add by martin 20140701 start
		queryContext.queryParam = queryParam;
		// add by martin 20140701 end
		
		if (parser.groupByDimensions.isEmpty() && parser.threshold == -1) {
			queryContext.query = Druids
					.newTimeseriesQueryBuilder()
					.dataSource(parser.getRoutedDataSource())
					.aggregators(
							new ArrayList<AggregatorFactory>(parser.aggregators
									.values()))
					.postAggregators(parser.postAggregators)
					.intervals(parser.intervals)
					.granularity(parser.granularity).filters(parser.filter)
					.build();

			queryContext.typeRef = new TypeReference<List<Result<TimeseriesResultValue>>>() {
			};
			queryContext.elemType = new TypeReference<Result<TimeseriesResultValue>>() {
			};
			queryContext.queryType = QueryType.TIMESERIES;
		} else if (parser.threshold != -1) {
			queryContext.query = new TopNQueryBuilder()
					.dataSource(parser.getRoutedDataSource())
					.dimension(parser.dimension)
					.metric(parser.metric)
					.threshold(parser.threshold)
					.intervals(parser.intervals)
					.filters(parser.filter)
					.granularity(parser.granularity)
					.aggregators(
							new ArrayList<AggregatorFactory>(parser.aggregators
									.values()))
					.postAggregators(parser.postAggregators).build();
			queryContext.typeRef = new TypeReference<List<Result<TopNResultValue>>>() {
			};
			queryContext.elemType = new TypeReference<Result<TopNResultValue>>() {
			};
			queryContext.queryType = QueryType.TOPN;

		} else {
			queryContext.query = GroupByQuery
					.builder()
					.setDataSource(parser.getRoutedDataSource())
					.setAggregatorSpecs(
							new ArrayList<AggregatorFactory>(parser.aggregators
									.values()))
					.setPostAggregatorSpecs(parser.postAggregators)
					.setInterval(parser.intervals)
					.setGranularity(parser.granularity)
					.setDimFilter(parser.filter)
					.setDimensions(
							new ArrayList<DimensionSpec>(
									parser.groupByDimensions.values()))
					.setLimitSpec(parser.orderBy).setLimit(parser.maxRows)
					.setHavingSpec(parser.having).build();

			queryContext.typeRef = new TypeReference<List<Row>>() {
			};
			queryContext.elemType = new TypeReference<MapBasedRow>() {
			};
			queryContext.queryType = QueryType.GROUPBY;

		}

		return queryContext;

	}

}
