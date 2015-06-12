package com.yeahmobi.datasystem.query.impala.jdbc.plugin;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import jersey.repackaged.com.google.common.collect.Lists;

import org.apache.log4j.Logger;

import ro.fortsoft.pf4j.Extension;
import ro.fortsoft.pf4j.Plugin;
import ro.fortsoft.pf4j.PluginWrapper;

import com.google.common.base.Splitter;
import com.yeahmobi.datasystem.query.exception.ReportRuntimeException;
import com.yeahmobi.datasystem.query.extensions.ImpalaJdbc;
import com.yeahmobi.datasystem.query.meta.GlobalCfg;
import com.yeahmobi.datasystem.query.meta.ImpalaAuthType;

/**
 * plugin of trading desk read file
 * 
 * @author ellis 2014.9.12
 */
public class Impala extends Plugin {
	private static Logger logger = Logger.getLogger(Impala.class);

	public Impala(PluginWrapper wrapper) {
		super(wrapper);
	}

	// port 21050 is the default impalad JDBC port
	private static final String IMPALAD_JDBC_PORT = "21050";

	// driver name
	private static final String JDBC_DRIVER_NAME = "org.apache.hive.jdbc.HiveDriver";

	private static final Router router = Router.getInstance();

	private static final String NO_AUTH_FORMAT = "jdbc:hive2://%s:%s/%s;auth=noSasl";
	private static final String LDAP_AUTH_FORMAT = "jdbc:hive2://%s:%s/%s;user=%s;password=%s";
	private static final String KOBEROS_AUTH_FORMAT = "jdbc:hive2://%s:%s/%s;principal=impala/%s@YEAHMOBI.COM";
	
	private static String getFullUrl(String database, String brokerUrl) {
		
		if(ImpalaAuthType.NO_AUTH == GlobalCfg.getInstance().getImpalaAuthType()){
			String fullUrl = String.format(NO_AUTH_FORMAT, brokerUrl,
					IMPALAD_JDBC_PORT, database);
			return fullUrl;
		}else if(ImpalaAuthType.LDAP == GlobalCfg.getInstance().getImpalaAuthType()){
			String fullUrl = String.format(LDAP_AUTH_FORMAT, brokerUrl,
					IMPALAD_JDBC_PORT, database, Router.getUsername(), Router.getPassword());
			return fullUrl;
		}else if(ImpalaAuthType.KOBEROS == GlobalCfg.getInstance().getImpalaAuthType()){
			String fullUrl = String.format(KOBEROS_AUTH_FORMAT, brokerUrl,
					IMPALAD_JDBC_PORT, database, brokerUrl);
			return fullUrl;
		}else{
			throw new RuntimeException("impala authenticate type is wrong " + GlobalCfg.getInstance().getImpalaAuthType());
		}
	}

	public static Connection conJdbcFactory(String database, String brokerUrl) {
		Connection con = null;
		try {
			Class.forName(JDBC_DRIVER_NAME);
			con = DriverManager.getConnection(getFullUrl(database, brokerUrl));
		} catch (Exception e) {
			logger.error("get connection failed", e);
		}
		return con;
	}

	@Extension
	public static class ImpalaJdbcImp implements ImpalaJdbc {

		@Override
		public List<List<Object>> query(String database, String sql) {
			List<List<Object>> rows = new LinkedList<>();

			String brokerUrl = null;
			Connection conn = null;
			Statement stmt = null;
			ResultSet rs = null;
			try {
				brokerUrl = router.getAvailuableNode();
				conn = conJdbcFactory(database, brokerUrl);
				stmt = conn.createStatement();
				
				rs = stmt.executeQuery(sql);
				ResultSetMetaData rsm = rs.getMetaData();
				int col = rsm.getColumnCount();

				while (rs.next()) {
					List<Object> row = new LinkedList<>();
					for (int i = 1; i <= col; i++) {
						row.add(rs.getObject(i));
					}
					rows.add(row);
				}
				router.enableNode(brokerUrl);
			} catch (Exception e) {
				router.disableOneHour(brokerUrl);
				logger.error(e.getClass(), e);
				throw new ReportRuntimeException(e, "failed to query Impala, sql is %s , url is %s", sql, brokerUrl);
			}finally{
				if(rs != null){
					try {
						rs.close();
					} catch (SQLException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
				
				if(stmt != null){
					try {
						stmt.close();
					} catch (SQLException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
				
				if(conn != null){
					try {
						conn.close();
					} catch (SQLException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
				
			}

			return rows;
		}
	}

	static class Router {

		/**
		 * 只要出现一次错误， 要等1小时后， 才能再次查询, 留下足够的维护时间
		 */
		private static final long TIME_OUT = 1L * 3600L * 60L * 1000L;

		private static final String CFG_NAME = "impala_sql_brokers.cfg";

		private static final String BOKERS_KEY = "realquery.impala.sql.brokers";

		private static final Properties properties = new Properties();

		private static Logger logger = Logger.getLogger(Router.class);

		private static int currentIndex = -1;

		private static List<Entry> brokersList = new ArrayList<>();

		private static String username = null;
		private static String password = null;
		private static Router INSTANCE = new Router();

		static {
			ClassLoader classLoader = Impala.class.getClassLoader();
			try {
				properties.load(classLoader.getResourceAsStream(CFG_NAME));
				String brokersStr = properties.getProperty(BOKERS_KEY);
				List<String> brokerStrList = Lists.newArrayList(Splitter.on(',').trimResults().split(brokersStr));

				for (int i = 0; i < brokerStrList.size(); ++i) {
					brokersList.add(new Entry(brokerStrList.get(i), -1L));
				}
				username = properties.getProperty("username").trim();
				password = properties.getProperty("password").trim();
				
			} catch (Exception e) {
				String msg = String.format("load impala brokers file %s failed", CFG_NAME);
				logger.error(msg, e);
			}
		}

		public static String getUsername() {
			return username;
		}

		public static String getPassword() {
			return password;
		}

		public static Router getInstance() {
			return INSTANCE;
		}

		private Router() {
		}

		public synchronized void enableNode(String nodeUrl) {
			// 只判断broker name
			int index = brokersList.indexOf(new Entry(nodeUrl, -1L));
			if (index != -1) {
				brokersList.set(index, new Entry(nodeUrl, -1L));
			}
		}

		/**
		 * 
		 * @return
		 */
		public synchronized String getAvailuableNode() {
			int actualIndex = -1;
			String url = null;
			for (int i = 1; i <= brokersList.size(); ++i) {
				actualIndex = (currentIndex + i) % brokersList.size();

				long curr = System.currentTimeMillis();
				long validFrom = brokersList.get(actualIndex).validFrom;

				if (curr > validFrom) {
					// 这个节点已经生效, 找到合适的节点
					url = brokersList.get(actualIndex).brokerUrl;
					break;
				}
			}

			// 所有节点都被disable 1个小时了
			if (null == url) {
				Collections.sort(brokersList);
				currentIndex = 0;
				return brokersList.get(0).brokerUrl;

			} else {
				currentIndex = actualIndex;
				return url;
			}
		}

		/**
		 * disable 1个小时
		 * 
		 * @param nodeUrl
		 */
		public synchronized void disableOneHour(String nodeUrl) {
			// find index
			Long curr = System.currentTimeMillis();
			Long validFrom = curr + TIME_OUT;
			// 设置validfrom值
			int index = brokersList.indexOf(new Entry(nodeUrl, validFrom));
			if (index != -1) {
				brokersList.set(index, new Entry(nodeUrl, validFrom));
			}
		}

		static class Entry implements Comparable<Entry> {
			public String brokerUrl;
			public Long validFrom;

			public Entry(String brokerName, Long validFrom) {
				this.brokerUrl = brokerName;
				this.validFrom = validFrom;
			}

			@Override
			public int compareTo(Entry o) {
				return this.validFrom.compareTo(o.validFrom);
			}

			public boolean equals(Object o) {
				if (!(o instanceof Entry)) {
					return false;
				}
				Entry rhs = (Entry) o;
				return this.brokerUrl.equals(rhs.brokerUrl);
			}
		}
	}
	public static void main(String[] args) {
		Impala.Router r = Impala.Router.getInstance();
		
		for(int i =0; i < 100; ++i){
			System.out.println(r.getAvailuableNode());
		}
	}
}
