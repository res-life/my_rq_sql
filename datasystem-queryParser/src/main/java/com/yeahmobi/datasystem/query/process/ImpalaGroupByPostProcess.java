package com.yeahmobi.datasystem.query.process;

/**
 * Created by yangxu on 3/17/14.
 */

import io.druid.data.input.Row;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.dimension.TypedDimensionSpec;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.yeahmobi.datasystem.query.antlr4.DruidReportParser;
import com.yeahmobi.datasystem.query.meta.DimensionTable;
import com.yeahmobi.datasystem.query.meta.MetricAggTable;
import com.yeahmobi.datasystem.query.meta.ReportPage;
import com.yeahmobi.datasystem.query.meta.ReportResult;
import com.yeahmobi.datasystem.query.skeleton.DataSourceViews;

public class ImpalaGroupByPostProcess extends PostProcessor {

	public ImpalaGroupByPostProcess(DruidReportParser parser) {
		super(parser);
	}

	@Override
	public ReportResult process(List<?> input) {

		ReportResult reportResult = new ReportResult();

		reportResult.setFlag("success");
		reportResult.setMsg("ok");

		@SuppressWarnings("unchecked")
		List<Row> rows = (List<Row>) input;
		
		reportResult.setPage(new ReportPage(parser.page, rows.size()));

		Iterable<String> dimensions = Iterables.transform(parser.groupByDimensions.values(), new Function<DimensionSpec, String>() {
			@Override
			public String apply(@Nullable DimensionSpec input) {
				return input.getOutputName();
			}
		});

		reportResult.append(Iterables.toArray(Iterables.concat(parser.intervalUnits, dimensions, parser.fields), String.class));

		String datasource = parser.getDataSource();
        DimensionTable dimensionTable = DataSourceViews.getViews().get(datasource).dimentions().getDimensionTable();
        MetricAggTable metricAggTable = DataSourceViews.getViews().get(datasource).metrics().getMetricAggTable();

		Map<String, DimensionSpec> timeDimensions = new LinkedHashMap<String, DimensionSpec>();
		if (parser.intervalUnits.size()>0) {
			for (String string : parser.intervalUnits) {
				timeDimensions.put(string, new TypedDimensionSpec(string, string, dimensionTable.getValueType(string).toDimType()));
			}
		}
		for (final Row r : rows) {
			reportResult.append(Iterables.toArray(
					Iterables.concat(Iterables.transform(timeDimensions.values(), new DimensionTransformer(r, dimensionTable)),
							Iterables.transform(parser.groupByDimensions.values(), new DimensionTransformer(r, dimensionTable)),
							Iterables.transform(parser.fields, new GroupByMetricTransformer(r, metricAggTable))), Object.class));

		}

		return reportResult;
	}
}
