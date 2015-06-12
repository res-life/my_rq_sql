package com.yeahmobi.datasystem.query.process;

import io.druid.data.input.MapBasedRow;
import io.druid.data.input.Row;
import io.druid.query.Result;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.timeseries.TimeseriesResultValue;
import io.druid.query.topn.DimensionAndMetricValueExtractor;
import io.druid.query.topn.TopNResultValue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;

import org.apache.log4j.Logger;
import org.joda.time.DateTime;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.yeahmobi.datasystem.query.antlr4.DruidReportParser;
import com.yeahmobi.datasystem.query.meta.DimensionTable;
import com.yeahmobi.datasystem.query.meta.MetricAggTable;
import com.yeahmobi.datasystem.query.meta.ReportPage;
import com.yeahmobi.datasystem.query.meta.ReportResult;
import com.yeahmobi.datasystem.query.meta.TableRef;
import com.yeahmobi.datasystem.query.reportrequest.DataRange;
import com.yeahmobi.datasystem.query.skeleton.DataSourceViews;

public class TopnPostProcessor extends PostProcessor {
    private static Logger logger = Logger.getLogger(TopnPostProcessor.class);

	public TopnPostProcessor(DruidReportParser parser) {
		super(parser);
	}

	@Override
	public ReportResult process(List<?> input) {
		ReportResult reportResult = new ReportResult();
		List<Object> rowo = new ArrayList<Object>();;
		Object res = null;
		
		reportResult.setFlag("success");
        reportResult.setMsg("ok");
        
        if(!(input.toString().equals("[]"))){
        	List<Result<TopNResultValue>> resulttopnlist = (List<Result<TopNResultValue>>) input;
        	Result<TopNResultValue> resulttopn = resulttopnlist.get(0);
    		DateTime datetime = resulttopn.getTimestamp();
    		TopNResultValue topnvalue = resulttopn.getValue();
    		List<DimensionAndMetricValueExtractor> topnvaluelist = topnvalue.getValue();
    		for (DimensionAndMetricValueExtractor dimensionAndMetricValueExtractor : topnvaluelist) {
    			rowo.add(new MapBasedRow(datetime, dimensionAndMetricValueExtractor.getBaseObject()));
			}
    		int start = DataRange.getStart(parser.page, parser.size, parser.offset);
    		int end = DataRange.getEnd(parser.page, parser.size, parser.offset);
            if (end <= rowo.size()||true) { // page starts from zero
                if (start > rowo.size() - 1) {
                    res = Collections.EMPTY_LIST;
                } else {
                    res = rowo.subList(start, end > rowo.size() ? rowo.size(): end);
                }
            }
        }else{
        	res = input;
        }
        
        List<Row> rows = (List<Row>)res;
//        List<Result<TopNResultValue>> rows = (List<Result<TopNResultValue>>) input;
//        List<Row> rows = (List<Row>) input;
        reportResult.setPage(new ReportPage(parser.page, rows.size()));
        Iterable<String> dimensions = Iterables.transform(parser.groupByDimensions.values(), new Function<DimensionSpec, String>() {
            @Override
            public String apply(@Nullable DimensionSpec input) {
                return input.getOutputName();
            }
        });

        reportResult.append(Iterables.toArray(
                Iterables.concat(parser.intervalUnits, dimensions, parser.fields),
                String.class)
        );

        String datasource = parser.getDataSource();
        DimensionTable dimensionTable = DataSourceViews.getViews().get(datasource).dimentions().getDimensionTable();
        MetricAggTable metricAggTable = DataSourceViews.getViews().get(datasource).metrics().getMetricAggTable();
//        DimensionTable dimensionTable = (DimensionTable)TableRef.getInstance().of(DimensionTable.class, parser.getDataSource());
//        MetricAggTable metricAggTable = (MetricAggTable)TableRef.getInstance().of(MetricAggTable.class, parser.getDataSource());
        for(final Row r : rows) {

                reportResult.append(
                        Iterables.toArray(
                                Iterables.concat(
                                        Iterables.transform(parser.intervalUnits, new TimeStampTransformer(r.getTimestampFromEpoch(), parser.timeZone)),
                                        Iterables.transform(parser.groupByDimensions.values(), new DimensionTransformer(r, dimensionTable)),
                                        Iterables.transform(parser.fields, new GroupByMetricTransformer(r, metricAggTable))),
                                Object.class)
                );

        }

        return reportResult;
    }
        

}
