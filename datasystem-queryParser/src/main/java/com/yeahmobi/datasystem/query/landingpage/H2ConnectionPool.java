package com.yeahmobi.datasystem.query.landingpage;

/**
 * Created by oscar.gao on 8/4/14.
 */

import org.h2.jdbcx.JdbcConnectionPool;

import com.yeahmobi.datasystem.query.meta.DatabaseCfg;

/**
 * JDBC connection pool
 * 
 */
public class H2ConnectionPool {

	// 数据库连接池 单例
	private static JdbcConnectionPool cp = null;

	static {
		// 从databaseCfg.json读取数据的Ip和Port
		String inMemoryDbHost = DatabaseCfg.getInstance().getHost();
		int inMemoryDbPort = DatabaseCfg.getInstance().getPort();

		// 生成Db连接的URL, 使用内存数据库方式
		String url = "jdbc:h2:tcp://" + inMemoryDbHost + ":" + inMemoryDbPort + "/mem:landingpage;DB_CLOSE_DELAY=-1";

		// 创建连接池
		cp = JdbcConnectionPool.create(url, "sa", "");
	}

	/**
	 * 获取数据库连接池
	 * @return
	 */
	public static JdbcConnectionPool getPool() {
		return cp;
	}
}
