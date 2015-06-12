package com.yeahmobi.datasystem.query.akka;

/**
 * Created by yangxu on 5/16/14.
 */

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Provider;
import com.yeahmobi.datasystem.query.guice.JsonConfigurator;
import com.yeahmobi.datasystem.query.guice.PropertiesModule;
import org.apache.log4j.Logger;

import java.util.Properties;

public class QueryConfigProvider implements Provider<QueryConfig> {

	private static Logger logger = Logger.getLogger(QueryConfigProvider.class);

	private static QueryConfig cfg = null;
	private static Object LOCK = new Object();

	@Override
	public QueryConfig get() {

		if (null == cfg) {

			synchronized (LOCK) {

				if (null != cfg) {
					return cfg;
				}

				Injector injector = Guice.createInjector(
						new QueryConfigModule(), new PropertiesModule(
								"config.properties"));

				JsonConfigurator jsonConfigurator = injector
						.getInstance(JsonConfigurator.class);

				cfg = jsonConfigurator.configurate(
						injector.getInstance(Properties.class),
						QueryConfig.CONFIG_BASE, QueryConfig.class);

				logger.info(cfg);

				return cfg;
			}
		} else {
			return cfg;
		}
	}

}
