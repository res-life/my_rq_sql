package com.yeahmobi.datasystem.query.meta;

import java.io.File;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.Map;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.yeahmobi.datasystem.query.serializer.ObjectSerializer;

/**
 * cache 的配置文件
 * 
 */
public class CacheCfg {

	// 是否使用 一级缓存
	private boolean enableL1 = true;

	// 是否使用 二级缓存
	private boolean enableL2 = true;

	// 清除一级缓存, 这是一个操作, 不参与equals
	private boolean clearL1;

	// 清除二级缓存， 这是一个操作, 不参与equals
	private boolean clearL2;

	// 容量设置，最大的 行*列, 默认值是1,000,000
	// [1,000,000 ~ 100,000,000,000]
	private long maxCapacityL2 = 1000000L;

	// 默认的cache时间单位5分钟
	// [5~3600]
	private int timeUnit = 300;

	public boolean isEnableL1() {
		return enableL1;
	}

	public void setEnableL1(boolean enableL1) {
		this.enableL1 = enableL1;
	}

	public boolean isEnableL2() {
		return enableL2;
	}

	public void setEnableL2(boolean enableL2) {
		this.enableL2 = enableL2;
	}

	public boolean isClearL1() {
		return clearL1;
	}

	public void setClearL1(boolean clearL1) {
		this.clearL1 = clearL1;
	}

	public boolean isClearL2() {
		return clearL2;
	}

	public void setClearL2(boolean clearL2) {
		this.clearL2 = clearL2;
	}

	public long getMaxCapacityL2() {
		return maxCapacityL2;
	}

	public void setMaxCapacityL2(long maxCapacityL2) {
		this.maxCapacityL2 = maxCapacityL2;
	}

	public int getTimeUnit() {
		return timeUnit;
	}

	public void setTimeUnit(int timeUnit) {
		this.timeUnit = timeUnit;
	}

	@Override
	public String toString() {
		return Objects.toStringHelper(this.getClass()).add("enableL1", enableL1).add("enableL2", enableL2).add("maxCapacityL2", maxCapacityL2).add("timeUnit", timeUnit).toString();
	}

	/**
	 * 得到全局cache设置
	 * @return
	 */
	public synchronized static CacheCfg getInstance() {
		return cfg;
	}

	/**
	 * 设置cache全局设置
	 * @param in
	 */
	public synchronized static void reset(CacheCfg in) {
		validate(in);
		CacheCfg.cfg.setEnableL1(in.enableL1);
		CacheCfg.cfg.setEnableL2(in.enableL2);
		CacheCfg.cfg.setMaxCapacityL2(in.maxCapacityL2);
		CacheCfg.cfg.setTimeUnit(in.timeUnit);

		ObjectSerializer.write(CacheCfg.class.getSimpleName() + ".json", cfg, CacheCfg.class.getClassLoader());
	}

	/**
	 * 校验设置
	 * @param in
	 */
	private static void validate(CacheCfg in) {
		Preconditions.checkArgument(in.getMaxCapacityL2() >= 1000L && in.getMaxCapacityL2() <= 100000000000L, "max L2 capacity must in [1000L, 100000000000L]", in.getMaxCapacityL2());
		Preconditions.checkArgument(in.getTimeUnit() >= 5 && in.getTimeUnit() <= 3600, "max L2 capacity must in [5, 3600]", in.getTimeUnit());
	}

	/**
	 * 全局变量
	 */
	private static volatile CacheCfg cfg = null;
	
	static{
		
		cfg = ObjectSerializer.read(CacheCfg.class.getSimpleName() + ".json", new TypeReference<CacheCfg>() { }, CacheCfg.class.getClassLoader());
		if(null == cfg){
			cfg = new CacheCfg();
		}			
	}
}
