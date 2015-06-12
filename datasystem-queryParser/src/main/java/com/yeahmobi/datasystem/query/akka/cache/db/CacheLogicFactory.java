package com.yeahmobi.datasystem.query.akka.cache.db;

import java.util.List;

/**
 * 工厂
 *
 */
public class CacheLogicFactory {

	// 工厂模式
	public static CacheLogic newInstance(List<String> timeDimentions) {
		return CacheLogicImp.newInstance(timeDimentions);
	}
}