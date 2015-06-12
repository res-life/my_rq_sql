package com.yeahmobi.datasystem.query.meta;

import java.util.LinkedHashMap;

import com.fasterxml.jackson.core.type.TypeReference;
import com.yeahmobi.datasystem.query.serializer.ObjectSerializer;

public class ImpalaCfg {

	private static volatile ClassLoader cfgClassLoader = null;

	public static void setCfgClassLoader(ClassLoader cfgClassLoader) {
		ImpalaCfg.cfgClassLoader = cfgClassLoader;
	}

	private LinkedHashMap<String, String> timeFunction;

	private LinkedHashMap<String, ImpalaCfgItem> datasources;

	public LinkedHashMap<String, String> getTimeFunction() {
		return timeFunction;
	}

	public void setTimeFunction(LinkedHashMap<String, String> timeFunction) {
		this.timeFunction = timeFunction;
	}

	public LinkedHashMap<String, ImpalaCfgItem> getDatasources() {
		return datasources;
	}

	public void setDatasources(LinkedHashMap<String, ImpalaCfgItem> datasources) {
		this.datasources = datasources;
	}

	private static final String JSON_FILE = "ImpalaCfg.json";
	private static volatile ImpalaCfg cfg = null;

	public static ImpalaCfg getInstance() {
		if (null == cfg) {
			cfg = ObjectSerializer.read(JSON_FILE, new TypeReference<ImpalaCfg>() {
			}, cfgClassLoader);
		}
		return cfg;
	}

	public static void reset(ImpalaCfg newCfg) {
		cfg = newCfg;
		ObjectSerializer.write(ImpalaCfg.class.getSimpleName() + ".json", newCfg, cfgClassLoader);
	}
	
	public static void main(String[] args) {
		ImpalaCfg a = ImpalaCfg.getInstance();
		System.out.println(a.timeFunction.get("day"));
	}

}
