
/**
 * Created by oscar.gao on 8/4/14.
 */

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 *
 */
public class H2InMemoryDbUtil {

	/**
	 * 
	 * @param sql
	 * @param errorMsg
	 * @throws ReportDbRuntimeException
	 */
	public static void executeDbStatement(String sql, String errorMsg) {

		Connection conn = null;
		Statement st = null;

		try {
			conn = H2ConnectionPool.getPool().getConnection();
			st = conn.createStatement();
			st.execute(sql);
		} catch(Exception e){
			System.out.println("error: " + e);
		}finally{
			closeAutoCloseable(st, "close database statement failed");
			closeAutoCloseable(conn, "close database connection failed");
		}
	}

	/**
	 * 
	 * @param tableName
	 * @param fields
	 * @param filter
	 * @return
	 * @throws ReportDbRuntimeException
	 */
	public static List<List<Object>> selectTable(String tableName, List<String> fields, String filter) {
		System.out.println("begin select");
		Connection conn = null;
		Statement st = null;
		ResultSet rs = null;
		try {
			System.out.println("begin select 1");
			conn = H2ConnectionPool.getPool().getConnection();
			System.out.println("begin select 2");
			st = conn.createStatement();
			System.out.println("begin select 3");
			String fieldsStr = join(fields);
			System.out.println("begin select 4");

			String sql = String.format("select %s from %s where %s", fieldsStr, tableName, filter);
			System.out.println(sql);
			rs = st.executeQuery(sql);
			List<List<Object>> rows = new LinkedList<>();
			while (rs.next()) {
				List<Object> row = new LinkedList<>();
				for (int i = 1; i <= fields.size(); ++i) {
					Object o = rs.getObject(i);
					row.add(o);
				}
				rows.add(row);
			}

			return rows;
		} catch(Exception e){
			System.out.println("error: " + e);
			return new ArrayList<>();
		}finally {
			closeAutoCloseable(rs, "close database result set failed");
			closeAutoCloseable(st, "close database statement failed");
			closeAutoCloseable(conn, "close database connection failed");
		}
	}

	private static String join(List<String> fields) {
		StringBuffer str = new StringBuffer(fields.get(0));
		for (int i = 1; i < fields.size(); ++i) {
			str.append("," + fields.get(i));
		}
		return str.toString();
	}

	/**
	 * 
	 * @param autoCloseable
	 * @param errorMsg
	 */
	private static void closeAutoCloseable(AutoCloseable autoCloseable, String errorMsg) {
		if (null != autoCloseable) {
			try {
				autoCloseable.close();
			} catch (Exception e) {
				System.out.println("error: " + e);
			}
		}
	}
}
