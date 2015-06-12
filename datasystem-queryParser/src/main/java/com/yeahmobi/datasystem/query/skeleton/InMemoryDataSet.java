package com.yeahmobi.datasystem.query.skeleton;

import io.druid.data.input.Row;
import io.druid.query.Result;
import io.druid.query.timeseries.TimeseriesResultValue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TimeZone;

import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineFactory;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

import org.joda.time.DateTime;
import org.joda.time.DateTimeField;
import org.joda.time.DateTimeFieldType;
import org.joda.time.DateTimeZone;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.yeahmobi.datasystem.query.meta.IntervalUnit;
import com.yeahmobi.datasystem.query.meta.TimeFilter;
import com.yeahmobi.datasystem.query.meta.TokenType;
import com.yeahmobi.datasystem.query.process.TimeStampTransformer;

/**
 * the druid result is saved into memory
 * 
 */
public class InMemoryDataSet extends AbstractDataSet {

	private List<Object> rows = new ArrayList<Object>();
	private PostContext request;

	public InMemoryDataSet(PostContext request) {
		this.request = request;
	}

	public InMemoryDataSet(PostContext request, List<Object> rows) {
		this.request = request;
		this.rows = rows;
	}
	
	
	@Override
	public void addRow(Object row) {
		if(!request.getParser().timeFilters.isEmpty()){
			Map<String , TimeFilter> timeFilters = request.getParser().timeFilters;
			DateTime time = null;
			switch (request.getQueryContext().getQueryType()) {
			case GROUPBY: {
				Row groupResult = (Row)row;
				time = new DateTime(groupResult.getTimestampFromEpoch());
				break;
			}
			case TIMESERIES: {
				Result<TimeseriesResultValue> timeResult = (Result<TimeseriesResultValue>)row;
				time = timeResult.getTimestamp();
				break;
			}
			default:
				break;
			}
			for (Entry<String, TimeFilter> iterable_element : timeFilters.entrySet()) {
				if(IntervalUnit.valueOf(iterable_element.getValue().dimension.toString().toUpperCase()).filterTime(time, iterable_element.getValue(),request.getParser().timeZone))
					return ;
			}
		}
		rows.add(row);

	}

	@Override
	public int size() {
		return rows.size();
	}

	@Override
	public List<Object> getAllData() {
		return rows;
	}

	public List<Object> subList(int fromIndex, int toIndex) {
		return rows.subList(fromIndex, toIndex);
	}

	@Override
	public DataSet subDataSet(int fromIndex, int toIndex) {
		return new InMemoryDataSet(request, subList(fromIndex, toIndex));
	}
	
	@Override
	public List<List<Object>> partition(int size) {
		return Lists.partition(rows, size);
	}
	
	@Override
	public List<DataSet> subDataSets(int size) {
		List<DataSet> list = new ArrayList<>();
		List<List<Object>> partitions = partition(size);
		for(List<Object> partition : partitions){
			list.add(new InMemoryDataSet(request, partition));
		}
		return list;
	}
	
	public static void main(String[] args) throws Exception {
		//System.out.println(IntervalUnit.valueOf("month".toUpperCase()).convert(DateTime.now().getMillis(), DateTimeZone.UTC));
		/*Iterables.transform(Arrays.asList("",""), new TimeStampTransformer(DateTime.now().getMillis(), DateTimeZone.UTC));
		Calendar c = Calendar.getInstance(); 
		c.setTime(DateTime.now().toDate()); 
		int week = c.get(Calendar.WEEK_OF_MONTH); 
		System.out.println(week);*/
	}
}
