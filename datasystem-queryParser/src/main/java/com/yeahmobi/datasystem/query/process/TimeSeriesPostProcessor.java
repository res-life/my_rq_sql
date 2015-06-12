package com.yeahmobi.datasystem.query.process;
/**
 * Created by yangxu on 3/17/14.
 */

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.primitives.Longs;
import com.metamx.common.ISE;
import com.yeahmobi.datasystem.query.antlr4.DruidReportParser;
import com.yeahmobi.datasystem.query.meta.*;
import com.yeahmobi.datasystem.query.skeleton.DataSourceViews;

import io.druid.data.input.MapBasedRow;
import io.druid.query.Result;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.aggregation.post.FieldAccessPostAggregator;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.dimension.TypedDimensionSpec;
import io.druid.query.groupby.orderby.DefaultLimitSpec;
import io.druid.query.groupby.orderby.LimitSpec;
import io.druid.query.groupby.orderby.OrderByColumnSpec;
import io.druid.query.groupby.orderby.TopNSorter;
import io.druid.query.timeseries.TimeseriesResultValue;

import org.apache.log4j.Logger;
import org.joda.time.DateTimeZone;

import javax.annotation.Nullable;

import java.util.*;

public class TimeSeriesPostProcessor extends PostProcessor {

    private static Logger logger = Logger.getLogger(TimeSeriesPostProcessor.class);

    public TimeSeriesPostProcessor(DruidReportParser parser) {
        super(parser);
    }

    @Override
    public ReportResult process(List<?> input) {

        ReportResult reportResult = new ReportResult();

        reportResult.setFlag("success");
        reportResult.setMsg("ok");

        List<Result<TimeseriesResultValue>> rows = (List<Result<TimeseriesResultValue>>) input;

        // no dimension
        reportResult.append(Iterables.toArray(
                Iterables.concat(parser.intervalUnits, parser.fields),
                String.class)
        );


        /*Function<List<Result<TimeseriesResultValue>>, List<Result<TimeseriesResultValue>>> postProcFn;
        postProcFn = buildFilter();
        if (null != postProcFn)
            rows = postProcFn.apply(rows);

        postProcFn = this.buildSorter(new ArrayList<AggregatorFactory>(parser.aggregators.values()),
                        parser.postAggregators,
                        parser.orderBy);

        if (null != postProcFn)
            rows = postProcFn.apply(rows);*/

        reportResult.setPage(new ReportPage(parser.page, rows.size()));

        String datasource = parser.getDataSource();
        DimensionTable dimensionTable = DataSourceViews.getViews().get(datasource).dimentions().getDimensionTable();
        MetricAggTable metricAggTable = DataSourceViews.getViews().get(datasource).metrics().getMetricAggTable();
//        DimensionTable dimensionTable = (DimensionTable)TableRef.getInstance().of(DimensionTable.class, parser.getDataSource());
//        MetricAggTable metricAggTable = (MetricAggTable)TableRef.getInstance().of(MetricAggTable.class, parser.getDataSource());
        
        Map<String, DimensionSpec> timeDoDb = new LinkedHashMap<String, DimensionSpec>();
        if(parser.isDoTimeDb){
        	for(String string : parser.intervalUnits){
        		timeDoDb.put(string,
        				new TypedDimensionSpec(string, string,
        						dimensionTable.getValueType(string).toDimType()));
        	}
        	parser.intervalUnits = new ArrayList<String>();
        }
        
        for(Result<TimeseriesResultValue> r : rows) {
                reportResult.append(
                        Iterables.toArray(
                                Iterables.concat(
                                        Iterables.transform(parser.intervalUnits, new TimeStampTransformer(r.getTimestamp().getMillis(), parser.timeZone)),
                                        Iterables.transform(timeDoDb.values(), new TimeSeriesMetricDimensionTran(r, dimensionTable)),
                                        Iterables.transform(parser.fields, new TimeSeriesMetricTransformer(r, metricAggTable))),
                                Object.class)
                );

        }
        
        parser.intervalUnits.addAll(parser.isDoTimeDb? timeDoDb.keySet() : Collections.EMPTY_LIST);
        return reportResult;
    }


    public Function<List<Result<TimeseriesResultValue>>, List<Result<TimeseriesResultValue>>> buildFilter() {

        if (parser.having != null) {
        return
                new  Function<List<Result<TimeseriesResultValue>>, List<Result<TimeseriesResultValue>>> () {
                    @Override
                    public List<Result<TimeseriesResultValue>> apply(@Nullable List<Result<TimeseriesResultValue>> input) {
                        if (null == input || input.isEmpty()) return Collections.EMPTY_LIST;
                        List<Result<TimeseriesResultValue>> results = new ArrayList<Result<TimeseriesResultValue>>();
                        for (Result<TimeseriesResultValue> row : input) {
                            if (parser.having.eval(new MapBasedRow(row.getTimestamp().getMillis(),
                                    row.getValue().getBaseObject()))) {
                                results.add(row);
                            }
                        }

                        return results;
                    }
                };
        }

        return null;
    }


    public Function<List<Result<TimeseriesResultValue>>, List<Result<TimeseriesResultValue>>> buildSorter(
            List<AggregatorFactory> aggs, List<PostAggregator> postAggs,
            LimitSpec limitSpec
    )
    {
        List<OrderByColumnSpec> columns = Collections.EMPTY_LIST;
        // TODO
        int limit = 50;
        if (limitSpec instanceof DefaultLimitSpec) {
            columns = ((DefaultLimitSpec)limitSpec).getColumns();
            limit = ((DefaultLimitSpec)limitSpec).getLimit();

        }

        if (columns.isEmpty()) {
            return new LimitingFn(limit);
        }

        // Materialize the Comparator first for fast-fail error checking.
        final Ordering<Result<TimeseriesResultValue>> ordering = makeComparator(aggs, postAggs,
                columns);

        if (limit == Integer.MAX_VALUE) {
            return new SortingFn(ordering);
        }
        else {
            return new TopNFunction(ordering, limit);
        }
    }

    private Ordering<Result<TimeseriesResultValue>> makeComparator(
            List<AggregatorFactory> aggs, List<PostAggregator> postAggs,
            List<OrderByColumnSpec> columns
    )
    {

        Ordering<Result<TimeseriesResultValue>> ordering = null;

        Map<String, Ordering<Result<TimeseriesResultValue>>> possibleOrderings = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);

        for (final AggregatorFactory agg : aggs) {
            final String column = agg.getName();
            possibleOrderings.put(column, metricOrdering(column, agg.getComparator()));
        }

        for (PostAggregator postAgg : postAggs) {
            String column = postAgg.getName();
            // FieldAccessPostAggregator support ordering
            if (postAgg instanceof FieldAccessPostAggregator) {
                possibleOrderings.put(column, metricOrdering(((FieldAccessPostAggregator) postAgg).getFieldName(), postAgg.getComparator()));
            } else {
                possibleOrderings.put(column, metricOrdering(column, postAgg.getComparator()));
            }
        }

        for (OrderByColumnSpec columnSpec : columns) {
            Ordering<Result<TimeseriesResultValue>> nextOrdering = possibleOrderings.get(columnSpec.getDimension());

            if (nextOrdering == null) {
                throw new ISE("Unknown column in order clause[%s]", columnSpec);
            }

            switch (columnSpec.getDirection()) {
                case DESCENDING:
                    nextOrdering = nextOrdering.reverse();
            }

            if (null == ordering) {
                ordering = nextOrdering;
            } else {
                ordering = ordering.compound(nextOrdering);
            }
        }
        if (null == ordering) {
            ordering = new Ordering<Result<TimeseriesResultValue>>()
            {
                @Override
                public int compare(Result<TimeseriesResultValue> left, Result<TimeseriesResultValue> right)
                {
                    return Longs.compare(left.getTimestamp().getMillis(), right.getTimestamp().getMillis());
                }
            };

        }

        return ordering;
    }

    private Ordering<Result<TimeseriesResultValue>> metricOrdering(final String column, final Comparator comparator)
    {
        return new Ordering<Result<TimeseriesResultValue>>()
        {
            @SuppressWarnings("unchecked")
            @Override
            public int compare(Result<TimeseriesResultValue> left, Result<TimeseriesResultValue> right)
            {
                return comparator.compare(left.getValue().getFloatMetric(column), right.getValue().getFloatMetric(column));
            }
        };
    }

    private Ordering<Result<TimeseriesResultValue>> timeOrdering(final IntervalUnit interval,
                                                                 final DateTimeZone tz)
    {
        return new Ordering<Result<TimeseriesResultValue>>()
        {
            @SuppressWarnings("unchecked")
            @Override
            public int compare(Result<TimeseriesResultValue> left, Result<TimeseriesResultValue> right)
            {
                return Ordering.natural().compare(
                        (String)interval.convert(left.getTimestamp(), tz),
                        (String)interval.convert(right.getTimestamp(), tz)
                );
            }
        };
    }


    private static class LimitingFn implements Function<List<Result<TimeseriesResultValue>>, List<Result<TimeseriesResultValue>>>
    {
        private int limit;

        public LimitingFn(int limit)
        {
            this.limit = limit;
        }

        @Override
        public List<Result<TimeseriesResultValue>> apply(
                @Nullable List<Result<TimeseriesResultValue>> input
        )
        {
            if (null == input || input.isEmpty()) {
                return Collections.EMPTY_LIST;
            }
            if (input.size() <= limit) {
                return input;
            }
            return input.subList(0, limit);
        }
    }

    private static class SortingFn implements
            Function<List<Result<TimeseriesResultValue>>, List<Result<TimeseriesResultValue>>>
    {
        private final Ordering<Result<TimeseriesResultValue>> ordering;

        public SortingFn(Ordering<Result<TimeseriesResultValue>> ordering) {this.ordering = ordering;}

        @Override
        public List<Result<TimeseriesResultValue>> apply(@Nullable List<Result<TimeseriesResultValue>> input)
        {
            return ordering.sortedCopy(input);
        }
    }

    private static class TopNFunction implements
            Function<List<Result<TimeseriesResultValue>>, List<Result<TimeseriesResultValue>>>
    {
        private final TopNSorter<Result<TimeseriesResultValue>> sorter;
        private final int limit;

        public TopNFunction(Ordering<Result<TimeseriesResultValue>> ordering, int limit)
        {
            this.limit = limit;

            this.sorter = new TopNSorter<Result<TimeseriesResultValue>>(ordering);

        }

        @Override
        public List<Result<TimeseriesResultValue>> apply(
                @Nullable List<Result<TimeseriesResultValue>> input
        )
        {
            if (null == input) return Collections.EMPTY_LIST;
            return Lists.newArrayList(sorter.toTopN(input, limit));
        }
    }

}
