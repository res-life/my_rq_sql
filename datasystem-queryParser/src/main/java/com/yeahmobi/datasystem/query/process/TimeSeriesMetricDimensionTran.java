package com.yeahmobi.datasystem.query.process;
/**
 * Created by yangxu on 3/17/14.
 */

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

import com.google.common.base.Function;
import com.yeahmobi.datasystem.query.meta.DimensionTable;

import io.druid.data.input.Row;
import io.druid.query.Result;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.timeseries.TimeseriesResultValue;

import org.apache.log4j.Logger;

import javax.annotation.Nullable;

public class TimeSeriesMetricDimensionTran implements Function<DimensionSpec, Object> {

    private static Logger logger = Logger.getLogger(TimeSeriesMetricDimensionTran.class);

    public TimeSeriesMetricDimensionTran(
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
        
        if(dim.equalsIgnoreCase("month")){
        	res = getMonthName(res);
        }
        
        if(dim.equalsIgnoreCase("week")){
        	Object monStr = res.toString().substring(0, 2);
        	res = getMonthName(monStr) + res.toString().substring(2);
        }
        
        if (null == res) {
            return table.getDefaultValue(dim);
        }
        return res;
    }
    
    /*
     * 转化月份的函数，即把01转化成jan
     */
    private static String getMonthName(Object monthNumber){
    	SimpleDateFormat sdf = new SimpleDateFormat("MM");
        Date date = null;
		try {
			date = sdf.parse(monthNumber.toString());
		} catch (ParseException e) {
			logger.error("result parse error", e);
		}
        sdf = new SimpleDateFormat("MMM",Locale.US);
        return sdf.format(date);
    }
    
}