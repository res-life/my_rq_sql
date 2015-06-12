package com.yeahmobi.datasystem.query.extensions;

import java.util.Map;
import com.yeahmobi.datasystem.query.meta.IntervalUnit;

/**
 * get the time dimensions
 */
public interface IntervalTables {
	
	/**
	 * get the IntervalUnit map
	 * @return
	 */
	Map<String, IntervalUnit> getMap();

}
