package com.yeahmobi.datasystem.query.akka.cache.db;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;

import org.apache.log4j.Logger;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.yeahmobi.datasystem.query.landingpage.H2InMemoryDbUtil;
import com.yeahmobi.datasystem.query.meta.CacheCfg;

public class CacheLogicImp implements CacheLogic {

	private List<String> timeDimentions;

	public synchronized static CacheLogic newInstance(List<String> timeDimentions) {
		return new CacheLogicImp(timeDimentions);
	}

	private CacheLogicImp(List<String> timeDimentions) {
		this.timeDimentions = timeDimentions;
	}

	/**
	 * 
	 * @param c
	 * @return
	 */
	@Override
	public boolean deleteOldCachesForThis(CacheRecord c) {

		while(currentSumCapacity() + c.getCapacity() > CacheCfg.getInstance().getMaxCapacityL2()){
			
			CacheRecord record = getOneAccordingTimeout();
			if(null != record){
				delete(record);
			}else{
				break;
			}
		}

		if (currentSumCapacity() + c.getCapacity() <= CacheCfg.getInstance().getMaxCapacityL2()) {
			return true;
		} else {
			logger.error("can't release enough space from level 2 cache system");
			return false;
		}
	}

	@Override
	public void add(CacheRecord c) {

		if (deleteOldCachesForThis(c)) {
			// 插入一行
			List<List<Object>> rows = ImmutableList.<List<Object>> of(ImmutableList.<Object> of(c.getDataSource(), c
					.getQuery(), c.getId(), c.getTableFieldsStr(), c.getCreateTime(), c.getResultTable(), c
					.getCacheStatus().toString(), c.getTimeoutTime(), c.getCapacity(), c.getIsFullData()));

			H2InMemoryDbUtil.insertDataList(CacheRecord.getMetaTableName(), CacheRecord.getFieldTypeMap(), rows);
		}
	}

	@Override
	public void delete(CacheRecord record) {
		// 删除这行
		String removeRow = String.format("delete from %s where tablename = '%s'", CacheRecord.getMetaTableName(),
				record.getId());

		try{
			H2InMemoryDbUtil.executeDbStatement(removeRow, "");		
		}catch(Exception e){
			logger.error(" sql error: " + removeRow);
		}

		// drop table
		H2InMemoryDbUtil.executeDbStatement("drop table if exists " + record.getResultTable(), "");
	}

	public void update(CacheRecord c) {
		try {
			String sql = String
					.format("update %s set datasource = '%s', query = '%s', tablefields = '%s', createtime = %s, resulttable = '%s', cachestatus = '%s', timeouttime = %s, capacity = %s, isfulldata = '%s' where tablename = '%s'",
							CacheRecord.getMetaTableName(), c.getDataSource(), c.getQuery(), c.getTableFieldsStr(),
							c.getCreateTime(), c.getResultTable(), c.getCacheStatus(), c.getTimeoutTime(),
							c.getCapacity(), c.getIsFullData().toString(), c.getId());
			
			H2InMemoryDbUtil.executeDbStatement(sql, "");
		} catch (Exception e) {
			logger.error("update cache record failed", e);
		}
	}

	@Override
	public List<CacheRecord> getAll() {
		return query("cachestatus != 'NOT_CACHE'");
	}

	private CacheRecord getOneAccordingTimeout() {
		List<CacheRecord> l = query("cachestatus = 'READY' and timeouttime is not null order by timeouttime asc limit 1");
		if(l.size() > 0){
			return l.get(0);
		}else{
			return null;
		}
	}
	
	@Override
	public CacheRecord get(String id) {
		String filter = String.format("cachestatus != 'NOT_CACHE' and tablename = '%s'", id);
		List<CacheRecord> ret = query(filter);
		return ret.size() > 0 ? ret.get(0) : null;
	}

	public List<String> getIds() {

		return Lists.transform(getAll(), new Function<CacheRecord, String>() {
			@Override
			public String apply(CacheRecord input) {
				return input.getId();
			}
		});
	}

	@Override
	public List<CacheRecord> queryTimeout(long willTimeoutIn) {

		long curr = System.currentTimeMillis();
		long target = curr + willTimeoutIn;
		return query("cachestatus != 'NOT_CACHE' and timeouttime < " + target);
	}

	@Override
	public long currentSumCapacity() {

		Object capacity  = H2InMemoryDbUtil.selectOneColumn("select ifnull(sum(capacity), 0) from lp_meta where cachestatus != 'NOT_CACHE'");
		return Long.parseLong(capacity.toString());
	}

	private List<CacheRecord> query(String filter) {
		List<List<Object>> datas = H2InMemoryDbUtil.selectTable(CacheRecord.getMetaTableName(),
				CacheRecord.getDbFields(), filter);
		return Lists.transform(datas, new Function<List<Object>, CacheRecord>() {
			public CacheRecord apply(List<Object> in) {

				String dataSource = (String) in.get(0);
				String query = (String) in.get(1);
				String id = (String) in.get(2);
				String fieldTypeMapStr = (String) in.get(3);
				LinkedHashMap<String, DbFieldType> fieldsMap = genFieldTypeMap(fieldTypeMapStr);
				long createTime = in.get(4) == null ? 0L : (long) (in.get(4));
				String resultTable = (String) in.get(5);
				CacheStatus cacheStatus = CacheStatus.valueOf((String) in.get(6));
				long timeoutTime = in.get(7) == null ? 0L : (long) (in.get(7));
				long capacity = in.get(8) == null ? 0L : (long) (in.get(8));

				return CacheRecord.builder().dataSource(dataSource).query(query).id(id).tableFields(fieldsMap)
						.createTime(createTime).resultTable(resultTable).cacheStatus(cacheStatus)
						.timeoutTime(timeoutTime).capacity(capacity).build();
			}
		});
	}

	private LinkedHashMap<String, DbFieldType> genFieldTypeMap(String mapString) {
		LinkedHashMap<String, DbFieldType> map = new LinkedHashMap<>();
		String[] entries = mapString.split(",");
		for (String entry : entries) {
			String[] v = entry.split("=");
			map.put(v[0], DbFieldType.valueOf(v[1]));
		}

		// 特殊处理， 注意.
		map.remove("timestamp");
		for (String time : timeDimentions) {
			map.put(time, DbFieldType.VARCHAR);
		}

		return map;
	}

	public void deleteAll() {
		for (CacheRecord record : getAll()) {
			delete(record);
		}
	}

	public static void main(String[] args) {
		Object a = new CacheLogicImp(null).getOneAccordingTimeout();
		CacheRecord r = CacheRecord.builder().cacheStatus(CacheStatus.READY).timeoutTime(System.currentTimeMillis()).id("abc").build();
		new CacheLogicImp(null).add(r);
		System.out.println(a);
	}
	private final static Logger logger = Logger.getLogger(CacheLogicImp.class);
}
