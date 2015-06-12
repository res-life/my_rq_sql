package com.yeahmobi.datasystem.query.extensions;

import java.util.Map;

import com.yeahmobi.datasystem.query.meta.DimensionDetail;
import com.yeahmobi.datasystem.query.meta.DimensionTable;

/**
 * all the dimentions informations for one data source
 *
 */
public interface Dimentions {
	
	/**
	 * get the dimention map
	 * @return
	 */
	Map<String, DimensionDetail> getMap();
	
	/**
	 * get the dimensionTable
	 * @return
	 */
	 DimensionTable getDimensionTable();
}
