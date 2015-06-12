
/**
 * Created by oscar.gao on 8/4/14.
 */

import org.h2.jdbcx.JdbcConnectionPool;

/**
 * JDBC connection pool
 * 
 */
public class H2ConnectionPool {

	// 数据库连接池 单例
	private static volatile JdbcConnectionPool cp = null;

	/**
	 * 获取数据库连接池
	 * 
	 * @return
	 */
	public static JdbcConnectionPool getPool() {
		if (cp == null) {
			try {
				// 从databaseCfg.json读取数据的Ip和Port
				String inMemoryDbHost = "127.0.0.1";
				int inMemoryDbPort = 9092;

				// 生成Db连接的URL, 使用内存数据库方�?
				String url = "jdbc:h2:tcp://" + inMemoryDbHost + ":" + inMemoryDbPort
						+ "/mem:landingpage;DB_CLOSE_DELAY=-1";

				// 创建连接�?
				cp = JdbcConnectionPool.create(url, "sa", "");
			} catch (Exception e) {
				System.out.println("error " + e);
			}
		}
		return cp;
	}
}
