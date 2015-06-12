package com.yeahmobi.datasystem.query.antlr4;

import java.util.LinkedHashMap;

public class SqlContext {

	String sql;
	boolean isJoin;
	String transformedSql;

	LinkedHashMap<String, String> metrics;
	
	public String getSql() {
		return sql;
	}
	public boolean isJoin() {
		return isJoin;
	}
	public String getTransformedSql() {
		return transformedSql;
	}
	
	public SqlContext(String sql, boolean isJoin, String transformedSql, LinkedHashMap<String, String> metrics){
		this.sql = sql;
		this.isJoin = isJoin;
		this.transformedSql = transformedSql;
		this.metrics = metrics;
	}
}
