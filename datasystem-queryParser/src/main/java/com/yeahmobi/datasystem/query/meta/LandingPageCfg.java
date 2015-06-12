package com.yeahmobi.datasystem.query.meta;

/**
 * Created by oscar.gao on 8/4/14.
 */

import java.util.LinkedHashMap;
import java.util.List;

import com.fasterxml.jackson.core.type.TypeReference;
import com.yeahmobi.datasystem.query.serializer.ObjectSerializer;

/**
 * the configuration file for landing page calculation
 * 
 */
public class LandingPageCfg {

	// the basic fields that used by metrics, like outs, clicks
	private List<String> basicFields;

	// like ctr = outs/clicks
	private LinkedHashMap<String, String> metrics;

	// can be used to group, like device_id, contry_id
	private List<String> groups;

	// others like sub1, ...
	private List<String> others;
	
	private static final String JSON_FILE = "LandingPageCfg.json";
	private static LandingPageCfg cfg = null;

	// parse the json file
	static {
		cfg = ObjectSerializer.read(JSON_FILE,
				new TypeReference<LandingPageCfg>() {
				});
	}

	// get the LandingPageTableCfg
	public static LandingPageCfg getInstance(){
		return cfg;
	}
	
	/**
	 * Getter
	 * 
	 * @return
	 */
	public List<String> getBasicFields() {
		return basicFields;
	}

	/**
	 * Setter
	 * 
	 * @param basicFields
	 */
	public void setBasicFields(List<String> basicFields) {
		this.basicFields = basicFields;
	}

	/**
	 * Getter
	 * 
	 * @return
	 */
	public LinkedHashMap<String, String> getMetrics() {
		return metrics;
	}

	/**
	 * Setter
	 * 
	 * @param metrics
	 */
	public void setMetrics(LinkedHashMap<String, String> metrics) {
		this.metrics = metrics;
	}

	/**
	 * Getter
	 * 
	 * @return
	 */
	public List<String> getOthers() {
		return others;
	}

	/**
	 * Setter
	 * 
	 * @param others
	 */
	public void setOthers(List<String> others) {
		this.others = others;
	}

	/**
	 * Getter
	 * 
	 * @return
	 */
	public List<String> getGroups() {
		return groups;
	}

	/**
	 * Setter
	 * 
	 * @param groups
	 */
	public void setGroups(List<String> groups) {
		this.groups = groups;
	}

	/**
	 * Test
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		LandingPageCfg a = ObjectSerializer.read(
				"LandingPageTableCfg.json",
				new TypeReference<LandingPageCfg>() {
				});
		System.out.println(a);
	}
}
