package com.yeahmobi.datasystem.query.akka.http;

/**
 * Created by oscar.gao on 8/4/14.
 */

import java.nio.charset.Charset;
import java.util.Map;

import org.apache.log4j.Logger;

import akka.actor.ActorRef;

import com.ning.http.client.AsyncHandler;
import com.ning.http.client.HttpResponseBodyPart;
import com.ning.http.client.HttpResponseHeaders;
import com.ning.http.client.HttpResponseStatus;
import com.yeahmobi.datasystem.query.akka.cache.CacheTool;
import com.yeahmobi.datasystem.query.akka.cache.Ttls;

import com.yeahmobi.datasystem.query.akka.cache.db.CacheLogic;
import com.yeahmobi.datasystem.query.akka.cache.db.CacheLogicFactory;
import com.yeahmobi.datasystem.query.akka.cache.db.CacheRecord;
import com.yeahmobi.datasystem.query.akka.cache.db.CacheStatus;
import com.yeahmobi.datasystem.query.antlr4.DruidReportParser;
import com.yeahmobi.datasystem.query.exception.ReportRuntimeException;
import com.yeahmobi.datasystem.query.jersey.ReportServiceResult;
import com.yeahmobi.datasystem.query.landingpage.H2InMemoryDbUtil;
import com.yeahmobi.datasystem.query.meta.ReportResult;
import com.yeahmobi.datasystem.query.process.QueryContext;
import com.yeahmobi.datasystem.query.reportrequest.ReportParam;
import com.yeahmobi.datasystem.query.skeleton.DataBaseDataSet;
import com.yeahmobi.datasystem.query.skeleton.DataSet;
import com.yeahmobi.datasystem.query.skeleton.DataSetHandler;
import com.yeahmobi.datasystem.query.skeleton.PostContext;
import com.yeahmobi.datasystem.query.skeleton.ReportResultDataSet;
import com.yeahmobi.datasystem.query.timedimension.TimeDimension;

/**
 * sync handler<br>
 * 在onCompleted中处理数据<br>
 * 
 */
public class DruidSyncHandler implements AsyncHandler<Boolean> {

	private ActorRef sender;
	private ActorRef receiver;
	private DruidReportParser parser;
	private QueryContext ctx;
	private CacheTool cacheTool;
	private TimeDimension timeDimension;
	private String timeTablename;
	private String groupTableName;
	private String groupJoinTableName;
	private int tableSize;

	// the result that returned to Client
	private ReportResult reportResult;
	private PostContext reportFeatures;
	private DataSet dataSet;
	private DataSetHandler dataSetHandler;
	private CacheTool cacheToolL2;

	// 
	private boolean isFirstPart = true;
	private boolean isDruidErrorOccured = false;
	private int druidSaveSize;
	
	/**
	 * constructor
	 * 
	 * @param parser
	 * @param ctx
	 * @param cacheTool
	 * @param sender
	 * @param receiver
	 * @param dataSetHandler
	 * @param landingPage
	 */
	public DruidSyncHandler(PostContext reportFeatures, DataSet dataSet, CacheTool cacheTool, CacheTool cacheToolL2,
			ActorRef sender, ActorRef receiver, DataSetHandler dataSetHandler) {
		this.reportFeatures = reportFeatures;
		this.dataSet = dataSet;
		this.parser = reportFeatures.getParser();
		this.ctx = reportFeatures.getQueryContext();
		this.cacheTool = cacheTool;
		this.cacheToolL2 = cacheToolL2;
		this.sender = sender;
		this.receiver = receiver;
		this.dataSetHandler = dataSetHandler;
	}

	/**
	 * when druid finished store result into in-memory db <br>
	 * will invoke this
	 */
	public Boolean onCompleted() throws Exception {


		if(isDruidErrorOccured){
			throw new ReportRuntimeException("Error occured: the druid result exceeds the max limit[500000] or other reason");
		}
		
		String id = reportFeatures.getTableName();
		druidSaveSize = H2InMemoryDbUtil.tableRowSize(id, "");
		
		DataSet retDataSet = dataSetHandler.processDataSet(dataSet);

		// 一级缓存省略， 因为数据库中有全部记录， 不用保存到一级缓存
		// 设置二级缓存
		setCacheL2(retDataSet);

		ReportServiceResult ret = retDataSet.getReportServiceResult();
		sender.tell(ret, receiver);
		return true;
	}

	private void setCacheL2(DataSet retDataSet) {
		// 保存二级缓存, 已经入库了， 因为是同步处理， 同步处理现在使用数据库保存
		if (cacheToolL2 != null) {
						
			Map<String, String> info = retDataSet.getInfo();
			String id = info.get("firstTableName");
			
			int limit = parser.maxRows;

			String resultTableName = info.get("lastTableName");
			ReportParam reportParam = reportFeatures.getReportParam();

			CacheLogic cacheLogic = CacheLogicFactory.newInstance(parser.intervalUnits);

			// 读取cache record
			CacheRecord record = cacheLogic.get(id);
			if (null == record) {
				logger.warn("can't do L2 cache, because of not find the record for id " + id);
				return;
			}

			String query = null;
			// 查询到的结果数小于limit, 说明是全量数据， 可以使用sort
			boolean isFullData = druidSaveSize < limit;
			if(isFullData){
				query = reportParam.toStringForL2WithoutSort();
			}else{
				query = reportParam.toStringForL2WithSort();
			}

			boolean isCached = false;
			long timeOutT = Ttls.Dynamic.apply(reportParam) * 1000;
			if(timeOutT < 10 * 60 * 1000){
				// 设置query 为一个查询不到的值, 相当于失效
				// 如果缓存时间小于10分钟， 则不适用L2 cache
				query = "not_cache_" + query;
				logger.info("real time query not use cache level 2 in sync handler, key is " + reportParam.toStringForL2WithSort());
				isCached = false;
			}else{
				// 使用L2 cache
				isCached = true;
			}

			record.setQuery(query);

			// 将cache 设置成 完成
			record.setCacheStatus(CacheStatus.READY);

			long createTime = System.currentTimeMillis();
			long timeoutTime = createTime + Ttls.Dynamic.apply(reportParam) * 1000;
			record.setTimeoutTime(timeoutTime);

			record.setResultTable(resultTableName);
			long capacity = H2InMemoryDbUtil.tableRowSize(resultTableName, "");
			record.setCapacity(capacity);
			String isFullDataStr = isFullData ? "true" : "false";
			record.setIsFullData(isFullDataStr);
			record.setId(id);
			
			// 更新到数据库， 设置timeout, cache status
			cacheLogic.deleteOldCachesForThis(record);

			cacheLogic.update(record);
			
			if(isCached){
				logger.info("save L2 cache successfully, already convert the data in the database to L2 cache");
				logger.info(String.format("cache level 2, key is %s, is full data %s", query, isFullData));
			}
		}
	}

	public STATE onBodyPartReceived(final HttpResponseBodyPart content) throws Exception {
		if(isFirstPart){
			// 第一个http 片段返回
			String str = new String(content.getBodyPartBytes(), "UTF-8").toLowerCase();
			if(str.contains("failure")){
				isDruidErrorOccured = true;
			}
			// 设置为false
			isFirstPart = false;
		}

		return STATE.CONTINUE;
	}

	public STATE onStatusReceived(final HttpResponseStatus status) throws Exception {
		logger.debug("the status from druid is " + status.getStatusCode());
    	if(status.getStatusCode() != 200){
    		throw new ReportRuntimeException("Error occured: the druid result exceeds the max limit[500000] or other reason");
    	}
		return STATE.CONTINUE;
	}

	public STATE onHeadersReceived(final HttpResponseHeaders headers) throws Exception {
		return STATE.CONTINUE;
	}

	@Override
	public void onThrowable(Throwable throwable) {
	}

	private final static Logger logger = Logger.getLogger(DruidSyncHandler.class);
}
