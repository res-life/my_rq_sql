package com.yeahmobi.datasystem.query.landingpage;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.List;

import org.h2.tools.Server;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.yeahmobi.datasystem.query.akka.cache.db.DbFieldType;

public class H2InMemoryDbUtilTest {

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

	@Test
	public void testFlow() {
		// 删除表如果存在: test
		dropTableIfExists();
		
		// 创建表: test
		createTable();
		
		// 生成数据["str1", 1.1, 1L], ["str2", 2.2, 2L], ["str3", 3.3, 3L]
		List<List<Object>> expected = genRows();
		
		// 生成: a=VARCHAR, b=DECIMAL, c=BIGINT
		LinkedHashMap<String, DbFieldType> typeMap = genFieldTypeMap();
		
		// 插入表
		H2InMemoryDbUtil.insertDataList("test", typeMap, expected);
		
		// 读取表
		List<List<Object>> actual = H2InMemoryDbUtil.readData("test", "", genFields(), 1, 3, "errorMsg");
		
		// assert
		Assert.assertEquals(expected, actual);
		
		int rowSize = H2InMemoryDbUtil.tableRowSize("test", "errorMsg");
		Assert.assertEquals(3, rowSize);
		
		rowSize = H2InMemoryDbUtil.tableRowSize("test", "c >= 2", "errorMsg");
		Assert.assertEquals(2, rowSize);
				
		actual = H2InMemoryDbUtil.selectTable("test", genFields(), "c <= 2");
		Assert.assertEquals(2, actual.size());
		
		// 删除表
		H2InMemoryDbUtil.executeDbStatement("drop table test", "errorMsg");
	}

	private static void createTable() {
		H2InMemoryDbUtil.executeDbStatement("create table test(a varchar, b decimal, c bigint)", "errorMsg");
	}

	private static void dropTableIfExists() {
		H2InMemoryDbUtil.executeDbStatement("drop table if exists test", "errorMsg");
	}

	public static LinkedHashMap<String, DbFieldType> genFieldTypeMap() {
		LinkedHashMap<String, DbFieldType> typeMap = new LinkedHashMap<>();
		typeMap.put("a", DbFieldType.VARCHAR);
		typeMap.put("b", DbFieldType.DECIMAL);
		typeMap.put("c", DbFieldType.BIGINT);
		return typeMap;
	}

	/**
	 * 生成三行数据
	 * @return
	 */
	private static List<List<Object>> genRows() {
		List<List<Object>> listList = ImmutableList.<List<Object>>of(ImmutableList.<Object> of("str1", new BigDecimal(1.1), 1L),
				ImmutableList.<Object> of("str2", new BigDecimal(2.2), 2L), ImmutableList.<Object> of("str3", new BigDecimal(3.3), 3L));
		return listList;
	}

	private static List<String> genFields(){
		return ImmutableList.of("a", "b", "c");
	}
	@Test
	public void testExecuteDbStatementFailed() {
		boolean isRuntimeDbExceptionOccured = false;
		try {
			H2InMemoryDbUtil.executeDbStatement("create table test(***_****)", "errorMsg");
		} catch (Exception e) {
			isRuntimeDbExceptionOccured = true;
		}

		Assert.assertTrue(isRuntimeDbExceptionOccured);
	}

	@Test
	public void TestReadDataFailed() {
		
		// 删除表如果存在: test
		dropTableIfExists();
		
		// 创建表: test
		createTable();
		
		boolean isRuntimeDbExceptionOccured = false;
		try {
			H2InMemoryDbUtil.readData("test*_*", "", genFields(), 1, 1, "errorMsg");
		} catch (Exception e) {
			isRuntimeDbExceptionOccured = true;
		}

		Assert.assertTrue(isRuntimeDbExceptionOccured);
	}
	
	@Test
	public void testSelectTableFailed(){
		// 删除表如果存在: test
		dropTableIfExists();
		
		// 创建表: test
		createTable();
		
		boolean isRuntimeDbExceptionOccured = false;
		try {
			H2InMemoryDbUtil.selectTable("test*_*", genFields(), "1=1");
		} catch (Exception e) {
			isRuntimeDbExceptionOccured = true;
		}

		Assert.assertTrue(isRuntimeDbExceptionOccured);
	}
	
	@Test
	public void testTableRowSizeFailed(){
		// 删除表如果存在: test
		dropTableIfExists();
		
		// 创建表: test
		createTable();
		
		boolean isRuntimeDbExceptionOccured = false;
		try {
			H2InMemoryDbUtil.tableRowSize("test", "invalid_filter", "errorMsg");
		} catch (Exception e) {
			isRuntimeDbExceptionOccured = true;
		}

		Assert.assertTrue(isRuntimeDbExceptionOccured);
	}
	
	@Test
	public void testInsertDataFailed(){
		// 删除表如果存在: test
		dropTableIfExists();
		
		// 创建表: test
		createTable();
		
		LinkedHashMap<String, DbFieldType> fieldTypeMap = genFieldTypeMap();
		// 设置type 为null
		fieldTypeMap.put("c", null);
		
		boolean isRuntimeDbExceptionOccured = false;
		try {
			H2InMemoryDbUtil.insertDataList("test", fieldTypeMap, genRows());
		} catch (Exception e) {
			isRuntimeDbExceptionOccured = true;
		}

		Assert.assertTrue(isRuntimeDbExceptionOccured);
	}
}
