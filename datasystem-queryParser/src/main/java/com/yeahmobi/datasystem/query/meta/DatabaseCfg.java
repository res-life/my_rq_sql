package com.yeahmobi.datasystem.query.meta;

import com.fasterxml.jackson.core.type.TypeReference;
import com.yeahmobi.datasystem.query.serializer.ObjectSerializer;

/**
 * Created by oscar.gao on 8/4/14.
 */

/**
 * 
 * database configuration <br>
 * this database is used to do special SQL calculation<br>
 * like landing page join SQL calculation<br>
 * or later some other special SQL calculation
 */
public class DatabaseCfg {

	private String host;
	private int port;

	private static final String CFG_FILE_NAME = "databaseCfg.json";
	private static DatabaseCfg cfg = null;

	// parse the JSON configuration file<br>
	// if JSON file not exist, will use default value<br>
	// default value is 127.0.0.1:9092
	static {
		cfg = ObjectSerializer.read(CFG_FILE_NAME,
				new TypeReference<DatabaseCfg>() {
				});
		if (null == cfg) {
			cfg = getDefaultCFG();
		}
	}

	/**
	 * parser
	 * @return
	 */
	public static DatabaseCfg getInstance() {
		return cfg;
	}

	/**
	 * default constructor
	 */
	public DatabaseCfg() {
	}

	/**
	 * constructor
	 * 
	 * @param host
	 *            host name
	 * @param port
	 *            IP port
	 */
	DatabaseCfg(String host, int port) {
		this.host = host;
		this.port = port;
	}

	/**
	 * Getter
	 * 
	 * @return
	 */
	public String getHost() {
		return host;
	}

	/**
	 * Setter
	 * 
	 * @param host
	 */
	public void setHost(String host) {
		this.host = host;
	}

	/**
	 * Getter
	 * 
	 * @return
	 */
	public int getPort() {
		return port;
	}

	/**
	 * Setter
	 * 
	 * @param port
	 */
	public void setPort(int port) {
		this.port = port;
	}

	private static final int DEFAULT_PORT = 9092;
	private static final String DEFAULT_IP = "127.0.0.1";
	static DatabaseCfg DEFAULT_CFG = new DatabaseCfg(DEFAULT_IP, DEFAULT_PORT);

	static DatabaseCfg getDefaultCFG() {
		return DEFAULT_CFG;
	}
	
	public static void main(String[] args){
		DatabaseCfg.getInstance();
	}
}
