package com.yeahmobi.datasystem.query.akka.cache.db;

/**
 * Created by yangxu on 3/17/14.
 */

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Locale;

import javax.annotation.Nullable;

import org.apache.log4j.Logger;

import com.google.common.base.Function;

public class MonthAndWeekTransformer implements Function<Object[], Object[]> {

	private static Logger logger = Logger.getLogger(MonthAndWeekTransformer.class);
	private List<String> header;

	public MonthAndWeekTransformer(List<String> header) {
		this.header = header;
	}

	@Nullable
	@Override
	public Object[] apply(@Nullable Object[] input) {

		int monthIndex = header.indexOf("month");
		int weekIndex = header.indexOf("week");

		if (monthIndex != -1) {
			input[monthIndex] = getMonthName(input[monthIndex]);
		}

		if (weekIndex != -1) {
			Object monStr = ((String) input[weekIndex]).substring(0, 2);
			input[weekIndex] = getMonthName(monStr) + ((String) input[weekIndex]).substring(2);
		}

		return input;
	}

	/*
	 * 转化月份的函数，即把01转化成jan
	 */
	private String getMonthName(Object monthNumber) {
		SimpleDateFormat sdf = new SimpleDateFormat("MM");
		Date date = null;
		try {
			date = sdf.parse(monthNumber.toString());
		} catch (ParseException e) {
			logger.error("result parse error", e);
		}
		sdf = new SimpleDateFormat("MMM", Locale.US);
		return sdf.format(date);
	}
}