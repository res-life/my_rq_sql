package com.yeahmobi.datasystem.query.meta;

import java.util.LinkedHashMap;
import java.util.List;

public class ImpalaCfgItem {

	private String useDatabase;

	private String targetTable;

	private LinkedHashMap<String, String> impala;

	public String getUseDatabase() {
		return useDatabase;
	}

	public void setUseDatabase(String useDatabase) {
		this.useDatabase = useDatabase;
	}

	public String getTargetTable() {
		return targetTable;
	}

	public void setTargetTable(String targetTable) {
		this.targetTable = targetTable;
	}

	public LinkedHashMap<String, String> getImpala() {
		return impala;
	}

	public void setImpala(LinkedHashMap<String, String> impala) {
		this.impala = impala;
	}
}
