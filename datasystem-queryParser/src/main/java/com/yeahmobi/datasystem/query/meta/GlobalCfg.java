package com.yeahmobi.datasystem.query.meta;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.Sets;
import com.yeahmobi.datasystem.query.akka.DruidSyncThreadPool;
import com.yeahmobi.datasystem.query.exception.ReportRuntimeException;
import com.yeahmobi.datasystem.query.impala.ImpalaThreadPool;
import com.yeahmobi.datasystem.query.serializer.ObjectSerializer;

public class GlobalCfg {

	// 同步情况下， 最多返回多少页
	private volatile int maxPageNumber = 20;

	
	// 最大的druid返回的rows,
	private volatile int maxSyncDruidResultRow = 100000;
	
	private volatile int maxAsyncDruidResultRow = 100000;
	

	public int getMaxSyncDruidResultRow() {
		return maxSyncDruidResultRow;
	}

	public void setMaxSyncDruidResultRow(int maxSyncDruidResultRow) {
		this.maxSyncDruidResultRow = maxSyncDruidResultRow;
	}

	public int getMaxAsyncDruidResultRow() {
		return maxAsyncDruidResultRow;
	}

	public void setMaxAsyncDruidResultRow(int maxAsyncDruidResultRow) {
		this.maxAsyncDruidResultRow = maxAsyncDruidResultRow;
	}


	private volatile int impalaCorePoolSize = 1;

	private volatile int impalaMaximumPoolSize = 2;

	private volatile ImpalaAuthType impalaAuthType = ImpalaAuthType.LDAP;


	private volatile int druidSyncRequestCorePoolSize = 3;
	private volatile int druidSyncRequestMaxPoolSize = 6;

	
	
	public int getDruidSyncRequestCorePoolSize() {
		return druidSyncRequestCorePoolSize;
	}

	public void setDruidSyncRequestCorePoolSize(int druidSyncRequestCorePoolSize) {
		this.druidSyncRequestCorePoolSize = druidSyncRequestCorePoolSize;
	}

	public int getDruidSyncRequestMaxPoolSize() {
		return druidSyncRequestMaxPoolSize;
	}

	public void setDruidSyncRequestMaxPoolSize(int druidSyncRequestMaxPoolSize) {
		this.druidSyncRequestMaxPoolSize = druidSyncRequestMaxPoolSize;
	}

	
	private volatile Map<String, Set> extractionDimensions = new HashMap<String, Set>(){{
																						    put("eve_druid_datasource_ds", Sets.newHashSet("click_time","conversion_time"));
																						    put("ymds_druid_datasource", Sets.newHashSet("click_time","conv_time"));
	                                                                                   }};
	public int getMaxPageNumber() {
		return maxPageNumber;
	}

	public void setMaxPageNumber(int maxPageNumber) {
		this.maxPageNumber = maxPageNumber;
	}

	public int getImpalaCorePoolSize() {
		return impalaCorePoolSize;
	}

	public void setImpalaCorePoolSize(int impalaCorePoolSize) {
		this.impalaCorePoolSize = impalaCorePoolSize;
	}

	public int getImpalaMaximumPoolSize() {
		return impalaMaximumPoolSize;
	}

	public void setImpalaMaximumPoolSize(int impalaMaximumPoolSize) {
		this.impalaMaximumPoolSize = impalaMaximumPoolSize;
	}

	public ImpalaAuthType getImpalaAuthType() {
		return impalaAuthType;
	}

	public void setImpalaAuthType(ImpalaAuthType impalaAuthType) {
		this.impalaAuthType = impalaAuthType;
	}

	public Map<String, Set> getExtractionDimensions() {
		return extractionDimensions;
	}

	public void setExtractionDimensions(Map<String, Set> extractionDimensions) {
		this.extractionDimensions = extractionDimensions;
	}

	/**
	 * 得到全局设置
	 * 
	 * @return
	 */
	public synchronized static GlobalCfg getInstance() {
		return cfg;
	}

	/**
	 * 设置全局设置
	 * 
	 * @param in
	 */
	public synchronized static void reset(GlobalCfg in) {
		validata(in);
		cfg.maxPageNumber = in.maxPageNumber;
		cfg.impalaMaximumPoolSize = in.impalaMaximumPoolSize;
		cfg.impalaCorePoolSize = in.impalaCorePoolSize;
		cfg.impalaAuthType = in.impalaAuthType;
		cfg.extractionDimensions = in.extractionDimensions;
		cfg.druidSyncRequestCorePoolSize = in.druidSyncRequestCorePoolSize;
		cfg.druidSyncRequestMaxPoolSize = in.druidSyncRequestMaxPoolSize;
		cfg.maxSyncDruidResultRow = in.maxSyncDruidResultRow;
		cfg.maxAsyncDruidResultRow = in.maxAsyncDruidResultRow;

		ObjectSerializer.write(GlobalCfg.class.getSimpleName() + ".json", cfg, GlobalCfg.class.getClassLoader());
		
		// 设置线程池
		ImpalaThreadPool.get().setCorePoolSize(cfg.getImpalaCorePoolSize());
		ImpalaThreadPool.get().setMaximumPoolSize(cfg.getMaxPageNumber());
		DruidSyncThreadPool.get().setCorePoolSize(cfg.getDruidSyncRequestCorePoolSize());
		DruidSyncThreadPool.get().setMaximumPoolSize(cfg.getDruidSyncRequestMaxPoolSize());
	}

	private static void validata(GlobalCfg in) {
		if (in.getImpalaCorePoolSize() > 1000 || in.getImpalaCorePoolSize() <= 0
				|| in.getImpalaMaximumPoolSize() < in.getImpalaCorePoolSize()) {
			throw new ReportRuntimeException(
					"parameter is wrong, impalaCorePoolSize is %s, impalaMaximumPoolSize is %s",
					in.getImpalaCorePoolSize(), in.getImpalaMaximumPoolSize());
		}
	}

	private static GlobalCfg cfg = null;
	static{
		cfg = ObjectSerializer.read(GlobalCfg.class.getSimpleName() + ".json", new TypeReference<GlobalCfg>() { }, GlobalCfg.class.getClassLoader());
		if(null == cfg){
			cfg = new GlobalCfg();
		}			
	}
}
