package com.yeahmobi.datasystem.query.akka.cache.db;

public enum CacheStatus {
	
	// 不是Cache
	NOT_CACHE,
	
	// 没有准备好， 不能查询
	NOT_READY,
	
	// cache 准备好了， 可以查询
	READY
}
