package com.yeahmobi.datasystem.query.akka.cache.db;

import java.util.List;

/**
 * cache 逻辑接口
 *
 */
public interface CacheLogic {

	/**
	 * 增加 cache, 如果已经满了， 则删除快要过期的
	 * @param cacheRecord
	 */
	void add(CacheRecord cacheRecord);
	
	/**
	 * 删除 cache, 先设置删除标记, 然后drop cache表, 再删除这行记录<br>
	 * 如果记录是正在使用状态， 则直接返回<br>
	 * @param cacheRecord
	 */
	void delete(CacheRecord cacheRecord);

	/**
	 * 删除所有的cache
	 */
	void deleteAll();
	
	/**
	 * update cache 状态
	 * @param cacheRecord
	 */
	void update(CacheRecord cacheRecord);
	
	/**
	 * 查找所有的id
	 * @return
	 */
	List<String> getIds();
	
	/**
	 * 返回所有的cache, 不包含NOT_CACHE
	 * @return
	 */
	List<CacheRecord> getAll();
	
	/**
	 * 根据id查cache
	 * @param id
	 * @return
	 */
	CacheRecord get(String id);
	
	/**
	 * 查询将要过期的cache
	 * 
	 * @param willTimeoutIn
	 *            单位毫秒
	 * @return
	 */
	List<CacheRecord> queryTimeout(long willTimeoutIn);

	/**
	 * 不包含not_cache的状态
	 * @return
	 */
	long currentSumCapacity();

	boolean deleteOldCachesForThis(CacheRecord c);
}