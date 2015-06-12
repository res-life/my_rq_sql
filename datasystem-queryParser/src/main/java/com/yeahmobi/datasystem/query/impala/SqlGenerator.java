package com.yeahmobi.datasystem.query.impala;

import io.druid.query.filter.AndDimFilter;
import io.druid.query.filter.DimFilter;
import io.druid.query.filter.NotDimFilter;
import io.druid.query.filter.OrDimFilter;
import io.druid.query.filter.RegexDimFilter;
import io.druid.query.filter.SelectorDimFilter;
import io.druid.query.groupby.having.AndHavingSpec;
import io.druid.query.groupby.having.EqualToHavingSpec;
import io.druid.query.groupby.having.GreaterThanHavingSpec;
import io.druid.query.groupby.having.HavingSpec;
import io.druid.query.groupby.having.LessThanHavingSpec;
import io.druid.query.groupby.having.NotHavingSpec;
import io.druid.query.groupby.having.OrHavingSpec;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.google.common.base.Joiner;
import com.yeahmobi.datasystem.query.meta.ValueType;

public class SqlGenerator {

	/**
	 *  生成filter语句<br>
	 *  NOTE regex is simple convert to '%parteen%'<br>
		so can't use complex regnex<br>
	 * @param filter
	 * @return
	 */
	public static String genFilterSql(DimFilter filter, Map<String, ValueType> types) {
		if (filter instanceof SelectorDimFilter) {
			SelectorDimFilter selector = (SelectorDimFilter) filter;
			String value = null;
			if (ValueType.STRING == types.get(selector.getDimension())) {
				value = "'" + selector.getValue() + "'";
			} else {
				value = selector.getValue();
			}

			return selector.getDimension() + "=" + value;
		} else if (filter instanceof RegexDimFilter) {
			RegexDimFilter regex = (RegexDimFilter) filter;
			
			String value = null;
			if (ValueType.STRING == types.get(regex.getDimension())) {
				
				// NOTE regex is simple convert to '%parteen%'
				// so can't use complex regnex
//				value = "'" + regex.getPattern().replaceAll("([^\\\\]\\\\{1})", "\\\\\\\\\\\\") + "'";
				value = "'" + regex.getPattern().replaceAll("\\\\{1}", "\\\\\\\\\\\\").replaceAll("\\\\{6}", "\\\\\\\\\\\\\\\\") + "'";
			} else {
				String errorMsg = String.format("regex dim filter %s can only filter string type but it's %s", regex, types.get(regex.getDimension()));
				throw new RuntimeException(errorMsg);
			}
			
			return regex.getDimension() + " regexp " + value;
		} else if (filter instanceof AndDimFilter) {
			AndDimFilter andFilter = (AndDimFilter) filter;
			List<String> subStrs = new ArrayList<>();
			for (DimFilter subFilter : andFilter.getFields()) {
				subStrs.add(genFilterSql(subFilter, types));
			}
			String body = Joiner.on(" and ").join(subStrs);
			return "(" + body + ")";
		} else if (filter instanceof NotDimFilter) {
			NotDimFilter sub = (NotDimFilter) filter;
			return "(not " + genFilterSql(sub.getField(), types) + ")";
		} else if (filter instanceof OrDimFilter) {
			OrDimFilter orFilter = (OrDimFilter) filter;
			List<String> subStrs = new ArrayList<>();
			for (DimFilter subFilter : orFilter.getFields()) {
				subStrs.add(genFilterSql(subFilter, types));
			}
			String body = Joiner.on(" or ").join(subStrs);
			return "(" + body + ")";
		} else {
			String errorMsg = String.format("filter %s type %s is not supported by impala sql", filter, filter.getClass());
			throw new RuntimeException(errorMsg);
		}
	}

	/**
	 * 生成have 语句<br>
	 * 
	 * @param havingSpec
	 * @return
	 */
	public static String genHavingSql(HavingSpec havingSpec) {
		if (havingSpec instanceof AndHavingSpec) {
			AndHavingSpec andSpec = (AndHavingSpec) havingSpec;

			List<String> subs = new ArrayList<>();
			for (HavingSpec subSpec : andSpec.getHavingSpecs()) {
				String subStr = genHavingSql(subSpec);
				subs.add(subStr);
			}

			String body = Joiner.on(" and ").join(subs);

			return "(" + body + ")";

		} else if (havingSpec instanceof EqualToHavingSpec) {
			EqualToHavingSpec equalSpec = (EqualToHavingSpec) havingSpec;

			return equalSpec.getAggregationName() + "=" + equalSpec.getValue();
		} else if (havingSpec instanceof GreaterThanHavingSpec) {
			GreaterThanHavingSpec great = (GreaterThanHavingSpec) havingSpec;
			return great.getAggregationName() + ">" + great.getValue();
		} else if (havingSpec instanceof LessThanHavingSpec) {
			LessThanHavingSpec less = (LessThanHavingSpec) havingSpec;
			return less.getAggregationName() + "<" + less.getValue();

		} else if (havingSpec instanceof NotHavingSpec) {
			NotHavingSpec not = (NotHavingSpec) havingSpec;

			return "(not " + genHavingSql(not.getHavingSpec()) + ")";
		} else if (havingSpec instanceof OrHavingSpec) {
			OrHavingSpec or = (OrHavingSpec) havingSpec;

			List<String> subs = new ArrayList<>();
			for (HavingSpec subSpec : or.getHavingSpecs()) {
				String subStr = genHavingSql(subSpec);
				subs.add(subStr);
			}

			String body = Joiner.on(" or ").join(subs);
			return "(" + body + ")";
		} else {
			String errorMsg = String.format("having spec %s type %s is not supported by impala sql", havingSpec, havingSpec.getClass());
			throw new RuntimeException(errorMsg);
		}
	}
}
