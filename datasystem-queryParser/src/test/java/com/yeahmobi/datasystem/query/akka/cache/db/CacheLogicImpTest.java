package com.yeahmobi.datasystem.query.akka.cache.db;

import java.sql.SQLException;
import java.util.Collections;
import java.util.LinkedHashMap;

import org.h2.tools.Server;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class CacheLogicImpTest {

	private static org.h2.tools.Server server;

	/**
	 * 启动h2数据库， 并绑定到127.0.0.1<br>
	 * 
	 * @throws SQLException
	 */
	@BeforeClass
	public static void init() throws SQLException {
		server = Server.createTcpServer().start();
	}

	/**
	 * 关闭h2数据库<br>
	 */
	@AfterClass
	public static void destroy() {
		server.shutdown();
	}
	
	@Before
	public void before() {
		CacheRecord.createMetaTableIfNotExists();
	}

	@Test
	public void test() {
		CacheRecord record1 = genCacheRecord("id_1");
		CacheRecord record2 = genCacheRecord("id_2");
		CacheLogic logic = CacheLogicFactory.newInstance(Collections.<String> emptyList());
		logic.add(record1);
		logic.add(record2);
		logic.getAll();
		logic.get(record1.getId());

		logic.delete(record1);
		logic.deleteAll();

	}

	public static CacheRecord genCacheRecord(String id) {
		return CacheRecord.builder().dataSource("datasource").query("query").id(id).tableFields(genFieldTypeMap())
				.createTime(System.currentTimeMillis()).resultTable("resulttable").cacheStatus(CacheStatus.READY)
				.timeoutTime(System.currentTimeMillis() + 5000L).capacity(10L).build();
	}

	private static LinkedHashMap<String, DbFieldType> genFieldTypeMap() {
		LinkedHashMap<String, DbFieldType> typeMap = new LinkedHashMap<>();
		typeMap.put("a", DbFieldType.VARCHAR);
		typeMap.put("b", DbFieldType.DECIMAL);
		typeMap.put("c", DbFieldType.BIGINT);
		return typeMap;
	}
}
