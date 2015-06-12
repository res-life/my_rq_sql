package com.yeahmobi.datasystem.query.akka.cache.db;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 用于记录执行时间
 * 
 */
public class CacheLogicMock implements CacheLogic {

	private List<CacheRecord> entries = null;

	private Map<String, Long> executeTimeInfo = new HashMap<>();

	@Override
	public void delete(CacheRecord cacheEntry) {
		final long curr = System.currentTimeMillis();
		if(executeTimeInfo.get(cacheEntry.getId()) == null){
			executeTimeInfo.put(cacheEntry.getId(), curr);
		}else{
			// already runninged
		}
	}

	@Override
	public List<CacheRecord> queryTimeout(final long willTimeoutIn) {
		final long curr = System.currentTimeMillis();

		List<CacheRecord> selected = new ArrayList<>();
		
		for (int i = 0; i < entries.size(); i++) {
			// 过滤
			long timeout = entries.get(i).getTimeoutTime();
			if (curr + willTimeoutIn > timeout) {
				selected.add(entries.get(i));
			}
		}
		
		return selected;
	}

	public Map<String, Long> getExecuteTimeInfo() {
		return executeTimeInfo;
	}

	public void setList(List<CacheRecord> entries) {
		this.entries = entries;
	}

	@Override
	public void add(CacheRecord cacheEntry) {
	}

	@Override
	public void deleteAll() {
	}

	@Override
	public void update(CacheRecord cacheRecord) {		
	}

	@Override
	public List<String> getIds() {
		return null;
	}

	@Override
	public List<CacheRecord> getAll() {
		return null;
	}

	@Override
	public CacheRecord get(String id) {
		return null;
	}

	@Override
	public long currentSumCapacity() {
		return 0;
	}

	@Override
	public boolean deleteOldCachesForThis(CacheRecord c) {
		// TODO Auto-generated method stub
		return false;
	}
}
