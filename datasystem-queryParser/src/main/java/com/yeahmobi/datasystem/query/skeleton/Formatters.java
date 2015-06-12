package com.yeahmobi.datasystem.query.skeleton;

import java.util.List;
import java.util.Map;

import org.rmrodrigues.pf4j.web.PluginManagerHolder;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import com.yeahmobi.datasystem.query.extensions.FormatterGenerator;

/**
 * used to collect all the data source plug-ins<br>
 * csv, json, xls, ...数据格式处理类
 * 
 */
public class Formatters {

	private static Map<String, FormatterGenerator> formatterGenerators;

	/**
	 * collect the data source plug-ins
	 */
	public static synchronized void init() {

		// get all the extions for the extension point
		List<FormatterGenerator> formatters = PluginManagerHolder
				.getPluginManager().getExtensions(FormatterGenerator.class);
		Builder<String, FormatterGenerator> dataSourcesBuilder = ImmutableMap
				.builder();
		for (FormatterGenerator formatter : formatters) {
			dataSourcesBuilder.put(formatter.getName(), formatter);
		}
		formatterGenerators = dataSourcesBuilder.build();
	}

	/**
	 * get the data source plug-in map key formatter value is the
	 * plug-in implements
	 * 
	 * @return
	 */
	public static synchronized Map<String, FormatterGenerator> getFormatters() {
		return formatterGenerators;
	}
}
