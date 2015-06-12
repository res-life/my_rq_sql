/**
 * 
 */
package com.yeahmobi.datasystem.query.config;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.log4j.Logger;

/**
 * @author yuanbinzou
 * 
 */
public class ConfigLoader {
	private PropertiesConfiguration configurations;
	private static Logger logger = Logger.getLogger(ConfigLoader.class);

    private final static ConfigLoader configLoader = new ConfigLoader();
	public static ConfigLoader getInstance() {
		return configLoader;
	}

	private ConfigLoader() {
		try {
			configurations = new PropertiesConfiguration("config.properties");
		} catch (ConfigurationException e) {
			logger.error("load config.propertirs error, use default value");
			logger.error("", e);
			return;
		}
	}

	public PropertiesConfiguration getConfigurations() {
		return configurations;
	}

	public void setConfigurations(PropertiesConfiguration configurations) {
		this.configurations = configurations;
	}

}
