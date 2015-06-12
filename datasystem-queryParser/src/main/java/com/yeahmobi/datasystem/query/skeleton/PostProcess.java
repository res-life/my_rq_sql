package com.yeahmobi.datasystem.query.skeleton;

import org.apache.log4j.Logger;

import akka.actor.ActorRef;

import com.ning.http.client.AsyncHandler;
import com.yeahmobi.datasystem.query.akka.cache.CacheTool;
import com.yeahmobi.datasystem.query.akka.http.DruidAsyncHandler;
import com.yeahmobi.datasystem.query.akka.http.DruidBodyAsyncConsumer;
import com.yeahmobi.datasystem.query.akka.http.DruidBodyAsyncDetailConsumer;
import com.yeahmobi.datasystem.query.akka.http.DruidSyncHandler;
import com.yeahmobi.datasystem.query.extensions.DataSourceView;
import com.yeahmobi.datasystem.query.extensions.FormatterGenerator;
import com.yeahmobi.datasystem.query.landingpage.H2InMemoryDbUtil;
import com.yeahmobi.datasystem.query.meta.DatabaseCfg;
import com.yeahmobi.datasystem.query.process.PostProcessor;
import com.yeahmobi.datasystem.query.process.PostProcessorFactory;
import com.yeahmobi.datasystem.query.utils.DbUtils;

public class PostProcess {

	private PostContext request;
	private ActorRef sender;
	private ActorRef receiver;
	private CacheTool cacheL1;
	private CacheTool cacheL2;
	private String brokerUrl;
	private String style;

	public static PostProcess createInstance(PostContext reportFeatures, ActorRef sender, ActorRef receiver,
			CacheTool cacheL1, CacheTool cacheL2, String url, String style) {
		return new PostProcess(reportFeatures, sender, receiver, cacheL1,
				cacheL2, url, style);
	}

	private PostProcess(PostContext reportFeatures, ActorRef sender, ActorRef receiver, CacheTool cacheL1,
			CacheTool cacheL2, String url, String style) {
		this.request = reportFeatures;
		this.sender = sender;
		this.receiver = receiver;
		this.cacheL1 = cacheL1;
		this.cacheL2 = cacheL2;
		this.brokerUrl = url;
	}

	/**
	 * 创建后处理链
	 * 
	 * @return
	 */
	private DataSetHandler createChineHandler() {
		// 1. create the common handler, 暂时只有时间维度
		DataSetHandler commonHandler = DataSetHandlerFactory.createHandler(request);

		// 2. get the extra handler for the specific data source
		DataSetHandler pluginHandler = null;
		DataSourceView dataSourceView = DataSourceViews.getViews().get(request.getParser().getDataSource());
		if (dataSourceView != null && request.getReportContext().isLp()) {
			pluginHandler = dataSourceView.getExtraHandler(request);
		} else {
			if (!request.getReportContext().isSyncProcess()) {
				pluginHandler = dataSourceView.getExtraHandler(request);
			} else {
				pluginHandler = new DefaultDataSetHandler();

			}
		}

		// 3. create the basic handler： 处理精度问题, 和其他问题, 对于不需要时间维度特殊处理的， 添加时间列
		PostProcessor postProcessor = PostProcessorFactory.create(request.getQueryContext().getQueryType(),
				request.getParser());
		BasicDataSetHandler basicHandler = new BasicDataSetHandler(request, postProcessor);

		// 4. 设置formatter的handler
		DataSetHandler formatDataSetHandler = new FormatterDataSetHandler(request.getReportContext().isSyncProcess(),
				request.getReportContext().getFormat(), request.getReportContext().getReportParam().getSettings()
						.getReport_id());

		// set the successor. 先时间维度, 再LP, 货币, 再基本的处理
		commonHandler.setSuccessor(basicHandler);
		basicHandler.setSuccessor(pluginHandler);
		pluginHandler.setSuccessor(formatDataSetHandler);

		return commonHandler;
	}

	/**
	 * 创建后处理链
	 * 
	 * @return
	 */
	private DataSetHandler createCommonChineHandler() {
		// 1. create the common handler, 暂时只有时间维度
		DataSetHandler commonHandler = DataSetHandlerFactory.createHandler(request);

		// 2. get the extra handler for the specific data source
		DataSetHandler pluginHandler = null;
		DataSourceView dataSourceView = DataSourceViews.getViews().get(request.getParser().getDataSource());
		if (dataSourceView != null && request.getReportContext().isLp()) {
			pluginHandler = dataSourceView.getExtraHandler(request);
		} else {
			if (!request.getReportContext().isSyncProcess()) {
				pluginHandler = dataSourceView.getExtraHandler(request);
			} else {
				pluginHandler = new DefaultDataSetHandler();

			}
		}

		// 3. create the basic handler： 处理精度问题, 和其他问题, 对于不需要时间维度特殊处理的， 添加时间列
		PostProcessor postProcessor = PostProcessorFactory.create(request.getQueryContext().getQueryType(),
				request.getParser());
		BasicDataSetHandler basicHandler = new BasicDataSetHandler(request, postProcessor);

		// 4. 设置formatter的handler
		DataSetHandler formatDataSetHandler = new FormatterDataSetHandler(request.getReportContext().isSyncProcess(),
				request.getReportContext().getFormat(), request.getReportContext().getReportParam().getSettings()
						.getReport_id());

		// set the successor. 先时间维度, 再LP, 货币, 再基本的处理
		commonHandler.setSuccessor(pluginHandler);
		pluginHandler.setSuccessor(basicHandler);
		basicHandler.setSuccessor(formatDataSetHandler);

		return commonHandler;
	}

	/**
	 * 创建两种数据集，并且在要入库的情况下生成表名及完成创表等操作。
	 * 
	 * @return 两种数据集(DataBaseDataSet和InMemoryDataSet)
	 */
	private DataSet createDataSet() {
		if (request.isInsertDb()) {
			String tableName = DbUtils.createUniqueTableName();

			request.setTableName(tableName);
			String dbUrl = DatabaseCfg.getInstance().getHost() + ":" + DatabaseCfg.getInstance().getPort();
			brokerUrl += "?saveToDb&&tablename=" + tableName + "&&db=" + dbUrl;

			try {
				H2InMemoryDbUtil.createDbTable(request, tableName);
			} catch (Exception e) {
				logger.error("create origal table failure and maybe cause by the jar of funtion", e);
			}
			logger.debug("create origal table " + tableName + " success");
			DataBaseDataSet dataBaseDataSet = new DataBaseDataSet(request, brokerUrl, tableName);
			dataBaseDataSet.setLastTableName(tableName);
			return dataBaseDataSet;
		}else {
			return new InMemoryDataSet(request);
		}
	}

	/**
	 * 创建Akka的处理对象， 其中包括后处理链
	 * 
	 * @return
	 */
	public AsyncHandler<Boolean> createAkkaHandler() {

		if (request.isInsertDb()) {
			return new DruidSyncHandler(request, createDataSet(), cacheL1, cacheL2, sender, receiver,
					createCommonChineHandler());
		} else {
			return new DruidAsyncHandler(new DruidBodyAsyncConsumer(request, createDataSet(), cacheL1, cacheL2, sender,
					receiver, createCommonChineHandler()));
		}
	}

	public AsyncHandler<Boolean> createFileAkkaHandler() {

		return new DruidAsyncHandler(new DruidBodyAsyncDetailConsumer(request, createDataSet(), null, sender, receiver,
				createChineHandler()));

	}

	public FormatterGenerator createFormatGenerator() {
		return null;
	}

	public String getBrokerUrl() {
		return brokerUrl;
	}

	private static final Logger logger = Logger.getLogger(PostProcess.class);
}
