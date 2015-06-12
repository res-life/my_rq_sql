package com.yeahmobi.datasystem.query.plugin;

import java.util.Map.Entry;

import junit.framework.TestCase;

import com.yeahmobi.datasystem.query.meta.DimensionDetail;
import com.yeahmobi.datasystem.query.meta.MetricDetail;
import com.yeahmobi.datasystem.query.plugin.YeahMobiPlugin.YeahMobiExtention;

public class YeahMobiPluginTest extends TestCase{

	public void test() {
		YeahMobiExtention ym = new YeahMobiExtention();
		String dataSourceName = ym.getPlugin().getDataSourceName();
		System.out.println("dataSourceName is "+dataSourceName);
		for(Entry<String, DimensionDetail> a : ym.getPlugin().getDataSourceView().dimentions().getMap().entrySet()){
			System.out.println(a.getValue().getName());
			System.out.println(a.getValue().getAlisa());
			System.out.println(a.getValue().getValueType().toString());
			System.out.println(a.getValue().getMaxLength());
			System.out.println(a.getValue().getDefaultValue().toString());
		}
	}
	
	public void test2() {
		YeahMobiExtention ym = new YeahMobiExtention();
		for(Entry<String, MetricDetail> a : ym.getPlugin().getDataSourceView().metrics().getMap().entrySet()){
			System.out.println(a.getValue().getName());
			System.out.println(a.getValue().getAlisa());
			System.out.println(a.getValue().getLevel());
			System.out.println(a.getValue().getPrecision());
			System.out.println(a.getValue().getFormula());
		}
	}

}
