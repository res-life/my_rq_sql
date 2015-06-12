package com.yeahmobi.datasystem.query.config;

/**
 * Created by yangxu on 3/20/14.
 * <p>
 * druid的broker主机信息（目前暂时不支持负载均衡的方式）
 */

public class Config {

    public static class Druid {

        static Druid defaultVal = new Druid("reportbroker.yeahmobi.com", "8080", "v2", "pretty");

        String host;
        String port;
        String ver;// druid目前使用v2
        String format;// 参数固定是pretty

        public String url() {
            return "http://" + host + ':' + port + "/druid/" + ver + "/?" + format;
        }

        public String getBaseUrl() {
            return "http://" + host + ':' + port + "/druid/" + ver + "/";
        }

        public String getSocket() {
            return host + ':' + port;
        }
        
        public Druid() {
        }

        Druid(String host, String port, String ver, String format) {
            this.host = host;
            this.port = port;
            this.ver = ver;
            this.format = format;
        }

        public String getHost() {
            return host;
        }

        public void setHost(String host) {
            this.host = host;
        }

        public String getPort() {
            return port;
        }

        public void setPort(String port) {
            this.port = port;
        }

        public String getVer() {
            return ver;
        }

        public void setVer(String ver) {
            this.ver = ver;
        }

        public String getFormat() {
            return format;
        }

        public void setFormat(String format) {
            this.format = format;
        }
    }
    
    public static class Router {

        static Router defaultVal = new Router("172.20.0.51", "8080", "datasystem-druid-router");
        String host;
        String port;
        String ver;

        public String url() {
            return "http://" + host + ':' + port + "/" + ver;
        }

        public String getBaseUrl() {
            return "http://" + host + ':' + port + "/" + ver + "/";
        }

        public String getSocket() {
            return host + ':' + port;
        }
        
        public Router() {
        }

        Router(String host, String port, String ver) {
            this.host = host;
            this.port = port;
            this.ver = ver;
        }

        public String getHost() {
            return host;
        }

        public void setHost(String host) {
            this.host = host;
        }

        public String getPort() {
            return port;
        }

        public void setPort(String port) {
            this.port = port;
        }

        public String getVer() {
            return ver;
        }

        public void setVer(String ver) {
            this.ver = ver;
        }

    }

    public Druid druid;

    public Druid getDruid() {
        return druid;
    }

    public void setDruid(Druid druid) {
        this.druid = druid;
    }
    
    public Router router;

	public Router getRouter() {
		return router;
	}

	public void setRouter(Router router) {
		this.router = router;
	}
    
}
