package com.yeahmobi.datasystem.query.extensions;

import java.util.List;

import ro.fortsoft.pf4j.ExtensionPoint;

/**
 * impala jdbc
 * 
 */
public interface ImpalaJdbc extends ExtensionPoint {

	List<List<Object>> query(String database, String sql);
}
