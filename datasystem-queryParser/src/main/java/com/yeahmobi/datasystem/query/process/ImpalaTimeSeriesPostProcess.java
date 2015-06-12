package com.yeahmobi.datasystem.query.process;
/**
 * Created by yangxu on 3/17/14.
 */

import com.google.common.collect.Iterables;
import com.yeahmobi.datasystem.query.antlr4.DruidReportParser;
import com.yeahmobi.datasystem.query.meta.*;
import com.yeahmobi.datasystem.query.skeleton.DataSourceViews;
import io.druid.query.Result;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.dimension.TypedDimensionSpec;
import io.druid.query.timeseries.TimeseriesResultValue;

import org.apache.log4j.Logger;
import java.util.*;

public class ImpalaTimeSeriesPostProcess extends PostProcessor {

    private static Logger logger = Logger.getLogger(ImpalaTimeSeriesPostProcess.class);

    public ImpalaTimeSeriesPostProcess(DruidReportParser parser) {
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

        reportResult.setPage(new ReportPage(parser.page, rows.size()));

        String datasource = parser.getDataSource();
        DimensionTable dimensionTable = DataSourceViews.getViews().get(datasource).dimentions().getDimensionTable();
        MetricAggTable metricAggTable = DataSourceViews.getViews().get(datasource).metrics().getMetricAggTable();
        
        Map<String, DimensionSpec> timeDoDb = new LinkedHashMap<String, DimensionSpec>();
        if(parser.intervalUnits.size()>0){
        	for(String string : parser.intervalUnits){
        		timeDoDb.put(string,
        				new TypedDimensionSpec(string, string,
        						dimensionTable.getValueType(string).toDimType()));
        	}
        }
        
        for(Result<TimeseriesResultValue> r : rows) {
                reportResult.append(
                        Iterables.toArray(
                                Iterables.concat(
                                        Iterables.transform(timeDoDb.values(), new TimeSeriesMetricDimensionTran(r, dimensionTable)),
                                        Iterables.transform(parser.fields, new TimeSeriesMetricTransformer(r, metricAggTable))),
                                Object.class)
                );

        }
        return reportResult;
    }

}