package com.yeahmobi.datasystem.query.akka;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;
import com.yeahmobi.datasystem.query.akka.cache.Ttls;
import com.yeahmobi.datasystem.query.meta.XchangeQueryType;
import com.yeahmobi.datasystem.query.meta.XchangeReturnFormat;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;

/**
 * Created by yangxu on 5/16/14.
 */

public class QueryConfig {

    @JsonIgnore
    public static final String CONFIG_BASE = "realquery.config";

    // time out in seconds for return current page to client
    @JsonProperty @Min(10) @Max(3600)
    private int timeout = 10;

    // conn time out in seconds for querying druid broker service
    @JsonProperty @Min(3) @Max(3600)
    private int connTimeout = 3;

    // socket time out in seconds for querying druid broker service
    @JsonProperty @Min(10) @Max(3600)
    private int socketTimeout = 10;

    // delay in milliseconds for starting query queue timer
    @JsonProperty @Min(10) @Max(3600)
    private int delay = 100;

    // period in milliseconds for starting query queue timer
    @JsonProperty @Min(1000) @Max(10000)
    private int period = 5000;
    
    // time out in minutes for return current page to client
    @JsonProperty @Min(6) @Max(60)
    private int asyncTimeout = 6;

    // actors for performing query
    @JsonProperty @Min(10) @Max(3600)
    private int actors = 10;

    // min number actors for performing query
    @JsonProperty @Min(1) @Max(3600)
    private int lowerBound = 10;

    // max number actors for performing query
    @JsonProperty @Min(100) @Max(3600)
    private int upperBound = 1000;

    // backend caching db
    @JsonProperty
    private String cacheType = "redis";
    
    @JsonProperty @Min(2) @Max(10000)
    private int requestDruidSyncBlockingQueueSize = 15;
    
    // aws s3 cfg
    @JsonProperty
    private String prefix = "s3://";
    @JsonProperty
    private String bucket = "druid.yeahmobi.com";
    @JsonProperty
    private String dir = "query.result/";
    @JsonProperty
    private String expir_dt_format = "yyyy-MM-dd HH:mm:ss";
    @JsonProperty
    private String expires = "2014-12-31 23:59:59";

    // function to calculating ttl of object to caching
    @JsonProperty
    private Ttls cacheTtlFunc = Ttls.Dynamic;
    
    // function to calculating ttl of object to caching
    @JsonProperty
    private String xchangeEnable = "disable";
    @JsonProperty
    private String xchangeSyncAPI = "http://172.20.0.51:9099/xchangeRates?params";
    @JsonProperty
    private XchangeQueryType xchangeSyncQueryType = XchangeQueryType.all;
    @JsonProperty
    private XchangeReturnFormat xchangeSyncReturnFormat = XchangeReturnFormat.json;
    @JsonProperty
    private String xchangeSyncColums = "currency_from,currency_to,rate_from_to,rate_usd_to";
    @JsonProperty
    private String xchangeableAggMetrics = "cost,revenue,profit,epc,rpc,cpc,arpa,acpa";
    @JsonProperty
    private String xchangeCacheType = "memcache";
    @JsonProperty
    private Ttls xchangeCacheTtlFunc = Ttls.HalfAnHour;

    // function to calculating ttl of object to caching
    @JsonProperty
    private boolean sentinelBroker = false;

    public int getTimeout() {
        return timeout;
    }

    public void setTimeout(int timeout) {
        this.timeout = timeout;
    }

    public int getActors() {
        return actors;
    }

    public void setActors(int actors) {
        this.actors = actors;
    }

    public int getLowerBound() {
        return lowerBound;
    }

    public void setLowerBound(int lowerBound) {
        this.lowerBound = lowerBound;
    }

    public int getUpperBound() {
        return upperBound;
    }

    public void setUpperBound(int upperBound) {
        this.upperBound = upperBound;
    }

    public String getCacheType() {
        return cacheType;
    }

    public void setCacheType(String cacheType) {
        this.cacheType = cacheType;
    }

    public String getPrefix() {
        return prefix;
    }

    public void setPrefix(String prefix) {
        this.prefix = prefix;
    }

    public String getBucket() {
        return bucket;
    }

    public void setBucket(String bucket) {
        this.bucket = bucket;
    }

    public String getDir() {
        return dir;
    }

    public void setDir(String dir) {
        this.dir = dir;
    }

    public String getExpires() {
        return expires;
    }

    public void setExpires(String expires) {
        this.expires = expires;
    }

    public String getExpir_dt_format() {
        return expir_dt_format;
    }

    public void setExpir_dt_format(String expir_dt_format) {
        this.expir_dt_format = expir_dt_format;
    }

    public Ttls getCacheTtlFunc() {
        return cacheTtlFunc;
    }

    public void setCacheTtlFunc(Ttls cacheTtlFunc) {
        this.cacheTtlFunc = cacheTtlFunc;
    }

    public String getXchangeSyncAPI() {
        return xchangeSyncAPI;
    }

    public void setXchangeSyncAPI(String xchangeSyncAPI) {
        this.xchangeSyncAPI = xchangeSyncAPI;
    }

    public String getXchangeEnable() {
        return xchangeEnable;
    }

    public void setXchangeEnable(String xchangeEnable) {
        this.xchangeEnable = xchangeEnable;
    }

    public XchangeQueryType getXchangeSyncQueryType() {
        return xchangeSyncQueryType;
    }

    public void setXchangeSyncQueryType(XchangeQueryType xchangeSyncQueryType) {
        this.xchangeSyncQueryType = xchangeSyncQueryType;
    }

    public XchangeReturnFormat getXchangeSyncReturnFormat() {
        return xchangeSyncReturnFormat;
    }

    public void setXchangeSyncReturnFormat(XchangeReturnFormat xchangeSyncReturnFormat) {
        this.xchangeSyncReturnFormat = xchangeSyncReturnFormat;
    }

    public String getXchangeSyncColums() {
        return xchangeSyncColums;
    }

    public void setXchangeSyncColums(String xchangeSyncColums) {
        this.xchangeSyncColums = xchangeSyncColums;
    }

    public String getXchangeableAggMetrics() {
        return xchangeableAggMetrics;
    }

    public void setXchangeableAggMetrics(String xchangeableAggMetrics) {
        this.xchangeableAggMetrics = xchangeableAggMetrics;
    }

    public Ttls getXchangeCacheTtlFunc() {
        return xchangeCacheTtlFunc;
    }

    public void setXchangeCacheTtlFunc(Ttls xchangeCacheTtlFunc) {
        this.xchangeCacheTtlFunc = xchangeCacheTtlFunc;
    }

    public String getXchangeCacheType() {
        return xchangeCacheType;
    }

    public void setXchangeCacheType(String xchangeCacheType) {
        this.xchangeCacheType = xchangeCacheType;
    }

    public boolean isSentinelBroker() {
        return sentinelBroker;
    }

    public void setSentinelBroker(boolean sentinelBroker) {
        this.sentinelBroker = sentinelBroker;
    }

    public int getAsyncTimeout() {
        return asyncTimeout;
    }

    public void setAsyncTimeout(int asyncTimeout) {
        this.asyncTimeout = asyncTimeout;
    }

    public int getConnTimeout() {
        return connTimeout;
    }

    public void setConnTimeout(int connTimeout) {
        this.connTimeout = connTimeout;
    }

    public int getSocketTimeout() {
        return socketTimeout;
    }

    public void setSocketTimeout(int socketTimeout) {
        this.socketTimeout = socketTimeout;
    }

    public int getDelay() {
        return delay;
    }

    public void setDelay(int delay) {
        this.delay = delay;
    }

    public int getPeriod() {
        return period;
    }

    public void setPeriod(int period) {
        this.period = period;
    }

    
    public int getRequestDruidSyncBlockingQueueSize() {
		return requestDruidSyncBlockingQueueSize;
	}

	public void setRequestDruidSyncBlockingQueueSize(int requestDruidSyncBlockingQueueSize) {
		this.requestDruidSyncBlockingQueueSize = requestDruidSyncBlockingQueueSize;
	}

	@Override
    public String toString() {
        return Objects.toStringHelper(this.getClass())
                .add("timeout", timeout)
                .add("connTimeout", connTimeout)
                .add("socketTimeout", socketTimeout)
                .add("delay", delay)
                .add("period", period)
                .add("actors", actors)
                .add("cacheType", cacheType)
                .add("prefix", prefix)
                .add("bucket", bucket)
                .add("dir", dir)
                .add("expir_dt_format", expir_dt_format)
                .add("expires", expires)
                .add("cacheTtlFunc", cacheTtlFunc)
                .add("xchangeSyncAPI", xchangeSyncAPI)
                .add("xchangeEnable", xchangeEnable)
                .add("xchangeSyncQueryType", xchangeSyncQueryType)
                .add("xchangeSyncReturnFormat", xchangeSyncReturnFormat)
                .add("xchangeSyncColums", xchangeSyncColums)
                .add("xchangeableAggMetrics", xchangeableAggMetrics)
                .add("xchangeCacheType", xchangeCacheType)
                .add("xchangeCacheTtlFunc", xchangeCacheTtlFunc)
                .add("sentinelBroker", sentinelBroker)
                .add("asyncTimeout", asyncTimeout)
                .toString();
    }
}
