package com.yeahmobi.datasystem.query.config;

import com.fasterxml.jackson.core.type.TypeReference;
import com.yeahmobi.datasystem.query.serializer.ObjectSerializer;

import org.apache.log4j.Logger;

/**
 * Created by yangxu on 3/20/14.
 * <p>
 * 读取配置文件信息Config.json
 * 
 * @author yangxu
 * 
 */
public class ConfigManager {

    private static Logger logger = Logger.getLogger(ConfigManager.class);

    private static ConfigManager instance = new ConfigManager();

    public static ConfigManager getInstance() {
        return instance;
    }

    private ConfigManager() {

    }

    Config cfg;

    public void init() {
        cfg = ObjectSerializer.read(Config.class.getSimpleName() + ".json", new TypeReference<Config>() {
        });

        if (null == cfg) {
            cfg = new Config();
            cfg.druid = Config.Druid.defaultVal;
            cfg.router = Config.Router.defaultVal;
        }

        logger.info(ObjectSerializer.write(cfg).toString());

    }

    public Config getCfg() {
        return cfg;
    }

}
