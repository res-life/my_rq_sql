package com.yeahmobi.datasystem.query.process;
/**
 * Created by yangxu on 3/17/14.
 */


import com.google.common.base.Function;
import com.yeahmobi.datasystem.query.meta.DimensionTable;

import io.druid.query.Result;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.timeseries.TimeseriesResultValue;

import org.apache.log4j.Logger;

import javax.annotation.Nullable;

public class TimeSeriesMetricDimensionTranIgnoreTime implements Function<DimensionSpec, Object> {

    private static Logger logger = Logger.getLogger(TimeSeriesMetricDimensionTranIgnoreTime.class);

    public TimeSeriesMetricDimensionTranIgnoreTime(
    		Result<TimeseriesResultValue> row,
            DimensionTable table
    ) {
        this.table = table;
        this.row = row;
    }
    

    final DimensionTable table;
    final Result<TimeseriesResultValue> row;
    @Nullable
    @Override
    public Object apply(@Nullable DimensionSpec input) {

        String dim = input.getOutputName().toLowerCase();
        Object res = row.getValue().getMetric(dim);
        
        if (null == res) {
            return table.getDefaultValue(dim);
        }
        return res;
    }
    
}