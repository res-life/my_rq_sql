package com.yeahmobi.datasystem.query.process;
/**
 * Created by yangxu on 3/17/14.
 */

import com.google.common.base.Function;
import com.yeahmobi.datasystem.query.meta.MetricAggTable;
import io.druid.query.Result;
import io.druid.query.timeseries.TimeseriesResultValue;
import org.apache.log4j.Logger;

import javax.annotation.Nullable;
import java.math.BigDecimal;

public class TimeSeriesMetricTransformer implements Function<String, Object> {

    private static Logger logger = Logger.getLogger(TimeSeriesMetricTransformer.class);

    public TimeSeriesMetricTransformer(Result<TimeseriesResultValue> row,
                                       MetricAggTable table
                                       ) {
        this.row = row;
        this.table = table;

    }
    final Result<TimeseriesResultValue> row;
    final MetricAggTable table;
    @Nullable
    @Override
    public Object apply(@Nullable String input) {
        return new BigDecimal(row.getValue().getDoubleMetric(input))
                .setScale(table.getAggPrecision(input), BigDecimal.ROUND_HALF_UP)
                ;
    }
}
