package com.yeahmobi.datasystem.query.config;

import org.apache.commons.configuration.ConversionException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

public class ConfigKeyConst {
	public static final String SERVICE_LISTEN_PORT = "service.listen.port";
	public static final String YEAHMOBI_SERVICE_CALLBACK = "yeahmobi.service.callback";
	
	public static final String MONGO_HOST_NAME = "mongo.host.name";
	public static final String MONGO_HOST_PORT = "mongo.host.port";
	public static final String MONGO_DB_NAMA = "mongo.db.nama";
	public static final String MONGO_DB_COLLECTION = "mongo.db.collection";
	public static final String MONGO_DB_USER_NAME = "mongo.db.user.name";
	public static final String MONGO_DB_USER_PASS = "mongo.db.user.pass";


	public static final String DATETIME_FORMAT_STYLE_YYYYMMDD = "datetime.format.style.yyyymmdd";
	public static final String DATETIME_FORMAT_STYLE_YYYYMMDDHHMM = "datetime.format.style.yyyymmddhhmm";
	public static final String DATETIME_FORMAT_STYLE_YYYYMMDDHHMMSS = "datetime.format.style.yyyymmddhhmmss";
	public static final String DATETIME_FORMAT_STYLE_YYYYMMDDHHMMSS_SSS = "datetime.format.style.yyyymmddhhmmss.sss";
	
	public static final String BROKER_REQUEST_CONNTIMEOUT = "broker.request.connTimeout";
	public static final String BROKER_REQUEST_SOCKETTIMEOUT = "broker.request.socketTimeout";
	public static final String QUERY_EXECUTE_TASK_PERIOD = "query.execute.task.period";
	public static final String QUERY_EXECUTE_TASK_DELAY = "query.execute.task.delay";

	/**
	 * set variable using given configuration unit
	 * 
	 * @param cfg
	 *            : configuration unit
	 * @param var
	 *            : original variable value
	 * @param key
	 *            : variable key in configuration unit
	 * @param fallback
	 *            : fallback value used when no config found if fallback ==
	 *            null, variable keeps its original value
	 */
	public static String parseStr(PropertiesConfiguration cfg, String var,
			String key, String fallback, Logger logger) {

		if (null == logger) {
			logger = Logger.getLogger("KeyConst.parseStr");
		}

		if (null == cfg) {
			logger.info("no config unit, use original(" + var + ")");
			return var;
		}

		String val = cfg.getString(key);
		// remove additional ";"
		val = StringUtils.removeEnd(val, ";");
		// if (StringUtil.isNotNullOrEmpty(val)) {
		if (StringUtils.isNotBlank(val)) {
			return val;
		} else {
			logger.info("No value of "
					+ key
					+ " in config file is found, using "
					+ ((fallback != null) ? "default(" + fallback : "original("
							+ var) + ") instead.");
			if (fallback != null) {
				return fallback;
			}
		}

		return var;
	}

	/**
	 * set variable using given configuration unit
	 * 
	 * @param cfg
	 *            : configuration unit
	 * @param var
	 *            : original variable value
	 * @param key
	 *            : variable key in configuration unit
	 * @param fallback
	 *            : fallback value used when no config found if fallback ==
	 *            null, variable keeps its original value
	 */
	public static int parseInt(PropertiesConfiguration cfg, int var,
			String key, String fallback, Logger logger) {
		if (null == logger) {
			logger = Logger.getLogger("KeyConst.parseInt");
		}

		if (null == cfg) {
			logger.info("no config unit, use original(" + var + ")");
			return var;
		}

		int val = var;
		if (cfg.containsKey(key)) {
			try {
				val = cfg.getInt(key);
			} catch (ConversionException e) {
				logger.error("The value of "
						+ key
						+ " in config file is incorrect, using "
						+ ((fallback != null) ? "default(" + fallback
								: "original(" + var) + ") instead.", e);
				if (fallback != null) {
					return Integer.parseInt(fallback);
				}
			}

		} else {
			logger.info("no value of "
					+ key
					+ " found in config file, using"
					+ ((fallback != null) ? "default(" + fallback : "original("
							+ var) + ") instead.");
			if (fallback != null) {
				return Integer.parseInt(fallback);
			}
		}

		return val;
	}
}
