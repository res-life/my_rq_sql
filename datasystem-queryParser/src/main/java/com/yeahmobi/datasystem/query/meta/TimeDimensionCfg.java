package com.yeahmobi.datasystem.query.meta;


import java.util.LinkedHashMap;
import com.fasterxml.jackson.core.type.TypeReference;
import com.yeahmobi.datasystem.query.serializer.ObjectSerializer;

/**
 * the configuration file for landing page calculation
 * 
 */
public class TimeDimensionCfg {

	private LinkedHashMap<String, String> yeahMobiMetrics;
	
	private LinkedHashMap<String, String> tradingDeskTimeMetrics;


	private static final String JSON_FILE = "timeDimensionPageCfg.json";
	private static TimeDimensionCfg cfg = null;

	// parse the json file
	static {
		cfg = ObjectSerializer.read(JSON_FILE,
				new TypeReference<TimeDimensionCfg>() {
				});
	}

	// get the LandingPageTableCfg
	public static TimeDimensionCfg getInstance(){
		return cfg;
	}

	public LinkedHashMap<String, String> getYeahMobiMetrics() {
		return yeahMobiMetrics;
	}

	public void setYeahMobiMetrics(LinkedHashMap<String, String> yeahMobiMetrics) {
		this.yeahMobiMetrics = yeahMobiMetrics;
	}

	public LinkedHashMap<String, String> getTradingDeskTimeMetrics() {
		return tradingDeskTimeMetrics;
	}

	public void setTradingDeskTimeMetrics(
			LinkedHashMap<String, String> tradingDeskTimeMetrics) {
		this.tradingDeskTimeMetrics = tradingDeskTimeMetrics;
	}
	


}
