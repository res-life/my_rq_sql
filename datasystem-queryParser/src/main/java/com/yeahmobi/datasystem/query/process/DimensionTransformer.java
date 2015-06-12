package com.yeahmobi.datasystem.query.process;

/**
 * Created by yangxu on 3/17/14.
 */

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

import io.druid.data.input.Row;
import io.druid.query.dimension.DimensionSpec;

import javax.annotation.Nullable;

import org.apache.log4j.Logger;

import com.google.common.base.Function;
import com.yeahmobi.datasystem.query.meta.DimensionTable;

/**
 * 默认值处理器
 * 
 */
public class DimensionTransformer implements Function<DimensionSpec, Object> {
	private static Logger logger = Logger.getLogger(DimensionTransformer.class);

	public DimensionTransformer(Row row, DimensionTable table) {
		this.table = table;
		this.row = row;
	}

	final DimensionTable table;
	final Row row;

	@Nullable
	@Override
	public Object apply(@Nullable DimensionSpec input) {
		String dim = input.getOutputName().toLowerCase();

		Object res = row.getRaw(dim);
		
		if(dim.equalsIgnoreCase("month")){
        	res = getMonthName(res);
        }
        
        if(dim.equalsIgnoreCase("week")){
        	Object monStr = res.toString().substring(0, 2);
        	res = getMonthName(monStr) + res.toString().substring(2);
        }
        
		if (null == res) {
			// 使用默认值
			return table.getDefaultValue(dim);
		}
		return res;
	}
	
	/*
     * 转化月份的函数，即把01转化成jan
     */
    private String getMonthName(Object monthNumber){
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
