package com.yeahmobi.datasystem.query.process;

/**
 * Created by yangxu on 3/17/14.
 */

import io.druid.data.input.Row;

import java.math.BigDecimal;

import javax.annotation.Nullable;

import com.google.common.base.Function;
import com.yeahmobi.datasystem.query.meta.MetricAggTable;

/**
 * 精度处理器
 *
 */
public class GroupByMetricTransformer implements Function<String, Object> {

	public GroupByMetricTransformer(Row row, MetricAggTable table) {
		this.row = row;
		this.table = table;
	}

	final Row row;
	final MetricAggTable table;

	@Nullable
	@Override
	public Object apply(@Nullable String input) {
		
		// 处理精度
		return new BigDecimal(String.valueOf(row.getRaw(input))).setScale(
				table.getAggPrecision(input), BigDecimal.ROUND_HALF_UP);
	}
}
