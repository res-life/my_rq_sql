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

import java.util.Arrays;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.yeahmobi.datasystem.query.meta.ValueType;

public class SqlGeneratorTest {

	@Test
	public void test() {

		HavingSpec havingSpec = genHaving();

		String havingStr = SqlGenerator.genHavingSql(havingSpec);
		Assert.assertEquals("((a=1 and b>2 and c<3) or (not d=4))", havingStr);

		DimFilter filter = genFilter();

		String filterStr = SqlGenerator.genFilterSql(filter, genTypes());
		Assert.assertEquals("((not (a='1' and b regexp '^2\\\\\\d')) or c=3)", filterStr);

	}

	private Map<String, ValueType> genTypes(){
		return ImmutableMap.<String, ValueType>of("a", ValueType.STRING, "b", ValueType.STRING, "c", ValueType.NUMBER);
	}
	
	private static HavingSpec genHaving() {
		HavingSpec equal = new EqualToHavingSpec("a", 1);
		HavingSpec great = new GreaterThanHavingSpec("b", 2);
		HavingSpec less = new LessThanHavingSpec("c", 3);
		HavingSpec and = new AndHavingSpec(Arrays.asList(equal, great, less));

		HavingSpec equal2 = new EqualToHavingSpec("d", 4);
		HavingSpec not = new NotHavingSpec(equal2);

		HavingSpec root = new OrHavingSpec(Arrays.asList(and, not));
		
		return root;
	}

	private static DimFilter genFilter() {
		DimFilter selectorFilter1 = new SelectorDimFilter("a", "1");
		DimFilter regexFilter1 = new RegexDimFilter("b", "^2\\d");
		
		DimFilter andFilter = new AndDimFilter(Arrays.asList(selectorFilter1, regexFilter1));
		DimFilter notFilter = new NotDimFilter(andFilter);

		DimFilter selectorFilter2 = new SelectorDimFilter("c", "3");
		DimFilter orFilter = new OrDimFilter(Arrays.asList(notFilter, selectorFilter2));
		return orFilter;
	}
}
