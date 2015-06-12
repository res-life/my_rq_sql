package com.yeahmobi.datasystem.query.akka;

/**
 * Created by yangxu on 5/5/14.
 */
import io.druid.jackson.DefaultObjectMapper;
import io.druid.query.filter.AndDimFilter;
import io.druid.query.filter.DimFilter;
import io.druid.query.filter.JavaScriptDimFilter;
import io.druid.query.filter.NotDimFilter;
import io.druid.query.filter.OrDimFilter;
import io.druid.query.filter.RegexDimFilter;
import io.druid.query.filter.SelectorDimFilter;
import io.druid.query.groupby.having.AndHavingSpec;
import io.druid.query.groupby.having.EqualToHavingSpec;
import io.druid.query.groupby.having.GreaterThanHavingSpec;
import io.druid.query.groupby.having.HavingSpec;
import io.druid.query.groupby.having.LessThanHavingSpec;
import io.druid.query.groupby.having.NotHavingSpec;
import io.druid.query.groupby.having.OrHavingSpec;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.log4j.Logger;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedActor;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.base.Stopwatch;
import com.google.common.base.Strings;
import com.google.gson.Gson;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.metamx.common.ISE;
import com.ning.http.client.AsyncHandler;
import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.AsyncHttpClientConfig;
import com.ning.http.client.ListenableFuture;
import com.yeahmobi.datasystem.query.StatementType;
import com.yeahmobi.datasystem.query.akka.cache.CacheTool;
import com.yeahmobi.datasystem.query.akka.cache.CacheToolChest;
import com.yeahmobi.datasystem.query.akka.cache.QueryCacheFactory;
import com.yeahmobi.datasystem.query.akka.cache.db.CacheToolL2;
import com.yeahmobi.datasystem.query.antlr4.DruidReportParser;
import com.yeahmobi.datasystem.query.antlr4.WarpInterpreter;
import com.yeahmobi.datasystem.query.config.ConfigManager;
import com.yeahmobi.datasystem.query.exception.FileErrorListener;
import com.yeahmobi.datasystem.query.exception.ReportParserException;
import com.yeahmobi.datasystem.query.exception.ReportRuntimeException;
import com.yeahmobi.datasystem.query.impala.ImpalaCallable;
import com.yeahmobi.datasystem.query.impala.ImpalaThreadPool;
import com.yeahmobi.datasystem.query.jersey.ReportServiceRequest;
import com.yeahmobi.datasystem.query.jersey.ReportServiceResult;
import com.yeahmobi.datasystem.query.meta.CacheCfg;
import com.yeahmobi.datasystem.query.meta.MsgType;
import com.yeahmobi.datasystem.query.meta.ReportPage;
import com.yeahmobi.datasystem.query.meta.ReportResult;
import com.yeahmobi.datasystem.query.meta.ReportResult.Entity;
import com.yeahmobi.datasystem.query.pretreatment.Pretreatment;
import com.yeahmobi.datasystem.query.pretreatment.ReportContext;
import com.yeahmobi.datasystem.query.process.QueryContext;
import com.yeahmobi.datasystem.query.process.QueryFactory;
import com.yeahmobi.datasystem.query.queue.QueryQueue;
import com.yeahmobi.datasystem.query.reportrequest.DataRange;
import com.yeahmobi.datasystem.query.reportrequest.Platform;
import com.yeahmobi.datasystem.query.reportrequest.ReportParam;
import com.yeahmobi.datasystem.query.reportrequest.ReportParamFactory;
import com.yeahmobi.datasystem.query.reportrequest.RouterRequest;
import com.yeahmobi.datasystem.query.reportrequest.RouterRequestFactory;
import com.yeahmobi.datasystem.query.reportrequest.RouterResult;
import com.yeahmobi.datasystem.query.skeleton.PostContext;
import com.yeahmobi.datasystem.query.skeleton.PostProcess;

public class QueryActor extends UntypedActor {

	CacheTool cacheTool = null;
	CacheTool cacheToolL2 = null;

	QueryConfig cfg;
	private PostContext postContext;

	@Override
	public void onReceive(Object message) throws Exception {

		try {
			if (message instanceof ReportServiceRequest) {
				// 执行查询
				query(((ReportServiceRequest) message), getSender(), getSelf());
			} else {
				unhandled(message);
			}
		} catch (Exception e) {
			// 通知客户端异常

			String msg = "Error occured: " + e.getClass().getName() + " " + e.getMessage();
			logger.error(msg, e);
			ReportServiceResult errorResult = new ReportServiceResult(MsgType.fail, msg);
			getSender().tell(errorResult, getSelf());
		}
	}

	private void query(ReportServiceRequest reportServiceRequest, ActorRef sender, ActorRef receiver) throws Exception {

		// 计时器开始
		Stopwatch stopwatch = Stopwatch.createStarted();

		// 获取原始请求和style
		String queryData = reportServiceRequest.getParam();
		String style = reportServiceRequest.getStyle().toLowerCase();
		StatementType queryType = reportServiceRequest.getQueryType();

		if(StatementType.JSON == queryType){
			
		}else if (StatementType.EASY_SQL == queryType){
			
		}else if(StatementType.SQL == queryType){
			
		}else{
			throw new ReportRuntimeException("statement type %s is unhandled", queryType);
		}
		

		// 将请求字符串转化成对象
		final ReportParam reportParam = ReportParamFactory.toObject(queryData);
		String report_id = reportParam.getSettings().getReport_id();
		final String dataSource = reportParam.getSettings().getData_source();
		ReportContext reportContext = new ReportContext(reportParam, style);

		// 记录原始请求
		if (logger.isDebugEnabled()) {
			logger.debug("report_id : [" + report_id + "].");
			logger.debug("queryData : [" + queryData + "].");
			logger.debug("style : [" + style + "].");
		}

		// 记录parse json request时间
		logger.info("[phase:parse-request][report id=" + report_id + "] --> "
				+ stopwatch.elapsed(TimeUnit.MILLISECONDS));

		// validate request
		validate(sender, receiver, report_id, dataSource);

		// 生成前处理责任链, 并执行
		Pretreatment.createChainAndExecute(reportContext);
		logger.info("[phase:pre-process][report id=" + report_id + "] --> " + stopwatch.elapsed(TimeUnit.MILLISECONDS));
		
		logger.info("[phase:request-router][query=" + report_id + "] --> " + stopwatch.elapsed(TimeUnit.MILLISECONDS));

		// 公式替换
		String queryParams = Pretreatment.replaceFormula(dataSource, reportParam);

		// 记录转化后的请求
		if (logger.isDebugEnabled()) {
			String msg = String.format("report id is %s the transformed request is [%s]", report_id, queryParams);
			logger.debug(msg);
		}

		// 语法解析
		final DruidReportParser parser = parseRequest(dataSource, queryParams);

		List<String> groupbys = new ArrayList<>(parser.groupByDimensions.keySet());
		List<String> aggregators = new ArrayList<>(parser.aggregators.keySet());
		List<String> filters = getFilterDimentions(parser.filter);
		List<String> havings = getHavingDimentions(parser.having);
		
		
		// 请求表路由服务
		RouterResult routerResult = routRequest(dataSource, reportParam.getSettings().getReport_id(), reportParam.getSettings().getTime().getStart(), reportParam.getSettings().getTime().getEnd(), groupbys, aggregators, filters, havings, parser.processType);

		// 判断是否使用impala
		boolean useImpala = Platform.IMPALA == routerResult.getPlatform();
		if(logger.isDebugEnabled()){
			logger.debug("useImpala=" + useImpala);
		}
		
		// 执行判断条件，直接往impala里面发送请求，目前先实现jdbc，接下来实现连接池
		// 重置parser的dataSource
		parser.setDataSource(dataSource);
		logger.info("[phase:parse][query=" + report_id + "] --> " + stopwatch.elapsed(TimeUnit.MILLISECONDS));

		// 赋上路由后的datasource和表路由后的url
		parser.setRoutedDataSource(routerResult.getDataSource());
		String url = routerResult.getBrokerUrl();
		
		// 创建druid查询对象
		QueryContext ctx = null;
		if(!useImpala)
			ctx = createDruidQueryContext(queryData, parser);
		logger.info("[phase:build][query=" + report_id + "] --> " + stopwatch.elapsed(TimeUnit.MILLISECONDS));

		// 这个对象保存所有的请求信息
		postContext = new PostContext(reportContext, ctx, parser);
		
		// 更改parser.isDoTimeDb
		parser.isDoTimeDb = postContext.useForParserDb() && postContext.useForParserResult();

		// 初始化cache tool, 如果缓存没有打开, 则初始化为null; 如果是impala查询， 需要借助l2 cache，
		// 所以l2必须打开
		genCacheTool(reportParam.getSettings().getReport_id(), useImpala);

		// 查找cache, 如果找到则直接返回
		ReportServiceResult cacheResult = findCache(postContext, cacheTool, cacheToolL2);
		if (cacheResult != null) {
			sender.tell(cacheResult, receiver);
			return;
		}

		// 如果impala开关是true
		if (useImpala) {
			if(postContext.getReportContext().isSyncProcess()){
				
				logger.debug("[impala:query][query=" + report_id + "] --> " + stopwatch.elapsed(TimeUnit.MILLISECONDS));
				
				// 放在线程池里执行
				ImpalaCallable callable = new ImpalaCallable(dataSource, parser);
				ReportResult ret = ImpalaThreadPool.get().submit(callable).get();
				
				ReportResult subResult = new GetResult().getPartBySize(ret, parser.page, parser.size, parser.offset);
				
				// 封装结果传给前台，目前只有json类型
				ReportServiceResult promptResult = new ReportServiceResult(MsgType.success, new Gson().toJson(subResult));
				
				sender.tell(promptResult, receiver);
				
				logger.debug("[impala:result][query=" + report_id + "] --> " + stopwatch.elapsed(TimeUnit.MILLISECONDS));
				
				// 得到没有转化week和月的结果
				ReportResult retForCache = callable.getReportResultForCache();
				
				// 所有页缓存到二级缓存
				if (cacheToolL2 != null) {
						int limit = parser.maxRows;
						int returnSize = ret.getData().getData().size() - 1;
						cacheToolL2.set(reportContext.getReportParam(), retForCache, returnSize < limit);						
				}
				logger.debug("[impala:cache][query=" + report_id + "] --> " + stopwatch.elapsed(TimeUnit.MILLISECONDS));
				return;
			}else{
				// 使用异步方式处理
				// 插入线程队列
				String format = postContext.getReportContext().getFormat();
				QueryQueue.addQuery(null, "impala query", url, true, dataSource, parser, format, postContext);

				// 立即返回
	            String msg = "You have committed a detail query, please download the query-result file after we calling back you.";
	    		ReportResult reportResult = new ReportResult();
	    		reportResult.setFlag("info");
	    		reportResult.setMsg(msg);
	    		reportResult.setData(null);
	            String s = new Gson().toJson(reportResult);
	            ReportServiceResult ret = new ReportServiceResult(MsgType.info, s);
	            sender.tell(ret, receiver);
			}

		}	
		
		// 生成druid的json 请求
		String queryStr = genDruidRequest(ctx);
		if (logger.isDebugEnabled()) {
			logger.debug("parser.orderBy [" + parser.orderBy + "].");
			String msg = String.format("report id is %s, request to druid cluster is [%s]", report_id, queryStr);
			String msgInfo = String.format("report id is %s, request to druid cluster", report_id);
			logger.info(msgInfo);
			logger.debug(msg);
		}

		if (Strings.isNullOrEmpty(url)) {
			// 如果是空， 使用默认值
			url = ConfigManager.getInstance().getCfg().getDruid().getBaseUrl();
		}

		// 向druid集群发请求； 或者插入线程队列， 等待后续处理
		requestDruid(sender, receiver, style, reportContext, url, queryStr);
		logger.info("[phase:done][query=" + report_id + "] --> " + stopwatch.elapsed(TimeUnit.MILLISECONDS));
	}

	private List<String> getHavingDimentions(HavingSpec havingSpec) {
		
		if(null == havingSpec){
			return new ArrayList<>();
		}
		
		if (havingSpec instanceof AndHavingSpec) {
			AndHavingSpec andSpec = (AndHavingSpec) havingSpec;
			List<String> tmpRet = new ArrayList<>();
			for (HavingSpec subSpec : andSpec.getHavingSpecs()) {
				tmpRet.addAll(getHavingDimentions(subSpec));
			}
			return tmpRet;
		} else if (havingSpec instanceof EqualToHavingSpec) {
			EqualToHavingSpec equalSpec = (EqualToHavingSpec) havingSpec;
			List<String> tmpRet = new ArrayList<>();
			tmpRet.add(equalSpec.getAggregationName());
			return tmpRet;
		} else if (havingSpec instanceof GreaterThanHavingSpec) {
			GreaterThanHavingSpec great = (GreaterThanHavingSpec) havingSpec;
			List<String> tmpRet = new ArrayList<>();
			tmpRet.add(great.getAggregationName());
			return tmpRet;
		} else if (havingSpec instanceof LessThanHavingSpec) {
			LessThanHavingSpec less = (LessThanHavingSpec) havingSpec;
			List<String> tmpRet = new ArrayList<>();
			tmpRet.add(less.getAggregationName());
			return tmpRet;

		} else if (havingSpec instanceof NotHavingSpec) {
			NotHavingSpec not = (NotHavingSpec) havingSpec;
			List<String> tmpRet = new ArrayList<>();
			tmpRet.addAll(getHavingDimentions(not.getHavingSpec()));
			return tmpRet;
		} else if (havingSpec instanceof OrHavingSpec) {
			OrHavingSpec or = (OrHavingSpec) havingSpec;
			List<String> tmpRet = new ArrayList<>();
			for (HavingSpec subSpec : or.getHavingSpecs()) {
				tmpRet.addAll(getHavingDimentions(subSpec));
			}
			return tmpRet;
		} else {
			String errorMsg = String.format("having spec %s type %s is not supported by impala sql", havingSpec, havingSpec.getClass());
			throw new RuntimeException(errorMsg);
		}
	}

	private List<String> getFilterDimentions(DimFilter filter) {
		if(null == filter){
			return new ArrayList<>();
		}
		
		if (filter instanceof SelectorDimFilter) {
			SelectorDimFilter selector = (SelectorDimFilter) filter;
			String dimension = selector.getDimension();
			List<String> tmpRet = new ArrayList<>();
			tmpRet.add(dimension);
			return tmpRet;
		} else if (filter instanceof RegexDimFilter) {
			RegexDimFilter regex = (RegexDimFilter) filter;
			List<String> tmpRet = new ArrayList<>();
			tmpRet.add(regex.getDimension());
			return tmpRet;
		} else if (filter instanceof JavaScriptDimFilter) {
			JavaScriptDimFilter jsFilter = (JavaScriptDimFilter) filter;
			List<String> tmpRet = new ArrayList<>();
			tmpRet.add(jsFilter.getDimension());
			return tmpRet;
		} else if (filter instanceof AndDimFilter) {
			AndDimFilter andFilter = (AndDimFilter) filter;
			List<String> tmpRet = new ArrayList<>();
			for (DimFilter subFilter : andFilter.getFields()) {
				tmpRet.addAll(getFilterDimentions(subFilter));
			}
			return tmpRet;
			
		} else if (filter instanceof NotDimFilter) {
			NotDimFilter sub = (NotDimFilter) filter;
			List<String> tmpRet = new ArrayList<>();
			tmpRet.addAll(getFilterDimentions(sub.getField()));
			return tmpRet;
		} else if (filter instanceof OrDimFilter) {
			OrDimFilter orFilter = (OrDimFilter) filter;
			List<String> tmpRet = new ArrayList<>();
			for (DimFilter subFilter : orFilter.getFields()) {
				tmpRet.addAll(getFilterDimentions(subFilter));
			}
			return tmpRet;
		} else {
			String errorMsg = String.format("filter %s type %s is not supported by impala sql", filter, filter.getClass());
			throw new RuntimeException(errorMsg);
		}
	}

	@SuppressWarnings("resource")
	private void requestDruid(ActorRef sender, ActorRef receiver, String style, ReportContext reportContext,
			String url, String queryStr) throws Exception{
		AsyncHttpClient client = null;
		try {

			AsyncHttpClientConfig.Builder builder = new AsyncHttpClientConfig.Builder();
			builder.setCompressionEnabled(true).setAllowPoolingConnection(true)
					.setRequestTimeoutInMs((int) TimeUnit.MINUTES.toMillis(cfg.getAsyncTimeout()))
					.setIdleConnectionTimeoutInMs((int) TimeUnit.MINUTES.toMillis(cfg.getAsyncTimeout()));

			client = new AsyncHttpClient(builder.build());

			// 后处理对象
			PostProcess postProcess = PostProcess.createInstance(postContext, sender, receiver, cacheTool, cacheToolL2, url, style);

			if (reportContext.isSyncProcess()) {
				// 同步操作
				requestDruidSync(queryStr, client, postProcess);
			} else {
				// 使用异步方式处理
				// 插入线程队列
				QueryQueue.addQuery(postProcess, queryStr, url, false, null, null, null, postContext);

				// 立即返回
	            String msg = "You have committed a detail query, please download the query-result file after we calling back you.";
	    		ReportResult reportResult = new ReportResult();
	    		reportResult.setFlag("info");
	    		reportResult.setData(null);
	    		reportResult.setMsg(msg);
	            String s = new Gson().toJson(reportResult);
	            ReportServiceResult ret = new ReportServiceResult(MsgType.info, s);
	            sender.tell(ret, receiver);
			}
		} finally {
			// 关闭资源
			if (null != client) {
				client.closeAsynchronously();
			}
		}
	}

	private void requestDruidSync(String queryStr, AsyncHttpClient client, PostProcess postProcess) throws Exception {

		String url;
		// 会创建两种akka handler
		AsyncHandler<Boolean> akkaHandler = postProcess.createAkkaHandler();
		// 重新获取最新url
		url = postProcess.getBrokerUrl();

		// 向druid集群发请求
		ListenableFuture<Boolean> future = client.preparePost(url).addHeader("content-type", "application/json")
				.setBody(queryStr.getBytes("UTF-8")).execute(akkaHandler);
		try {
			future.get();
		} catch (Exception e) {
			if(e instanceof java.util.concurrent.ExecutionException && null != e.getCause()){
				//
				throw new ReportRuntimeException(e, e.getCause().getMessage());
			}else{
				throw e;
			}
		}
	}

	/*
	 * 将druid查询对象转换成string
	 */
	private String genDruidRequest(QueryContext ctx) throws JsonProcessingException {
		String queryStr;
		ObjectMapper objectMapper = new DefaultObjectMapper();
		ObjectWriter jsonWriter = objectMapper.writerWithDefaultPrettyPrinter();
		//
		queryStr = jsonWriter.writeValueAsString(ctx.getQuery());
		return queryStr;
	}

	/**
	 * 创建对应的druid查询对象
	 * 
	 * @param queryData
	 * @param parser
	 * @return
	 */
	private QueryContext createDruidQueryContext(String queryData, DruidReportParser parser) {
		QueryContext ctx = null;
		try {
			ctx = QueryFactory.create(parser, queryData);
		} catch (ISE e) {

			logger.error(e.getMessage(), e);
			throw new RuntimeException("syntax error:build");
		}
		return ctx;
	}

	/**
	 * parse 请求
	 * 
	 * @param dataSource
	 * @param queryParams
	 * @return parser对象
	 */
	private DruidReportParser parseRequest(String dataSource, String queryParams) {
		DruidReportParser parser = null;
		try {
			parser = WarpInterpreter.convert(queryParams, dataSource, new FileErrorListener());
		} catch (ReportParserException e) {
			logger.error(e.getMessage(), e);
			throw new RuntimeException("syntax error:parse");
		}
		return parser;
	}

	/**
	 * 请求router
	 * 
	 * @param reportParam
	 * @return broker URL 和 datasource 的小表
	 */
	private RouterResult routRequest(String dataSource, String reportId, long start, long end, List<String> groupbys, List<String> aggregators, List<String> filters, List<String> havings, String processType) {
		
		
		RouterRequest req = new RouterRequest();
		req.setDataSource(dataSource);
		req.setReportId(reportId);
		req.setStart(start);
		req.setEnd(end);
		req.setAggregators(aggregators);
		req.setFilters(filters);
		req.setGroupbys(groupbys);
		req.setHavings(havings);
		req.setProcessType(processType);
		String request = RouterRequestFactory.toString(req);
		
		RouterResult routerResult= null;
		try {
			DefaultHttpClient routerClient = new DefaultHttpClient();
			String routerUrl = ConfigManager.getInstance().getCfg().getRouter().getBaseUrl() + "router";
			HttpPost httppost = new HttpPost(routerUrl);
			httppost.setHeader("content-type", "application/json");
			httppost.setEntity(new StringEntity("report_request=" + request));
			HttpResponse response = routerClient.execute(httppost);
			BufferedReader in = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));
			String line = null;
			if ((line = in.readLine()) != null) {
				ObjectMapper objectMapperReportParam = new DefaultObjectMapper();
				routerResult = objectMapperReportParam.readValue(line, RouterResult.class);
			}
		} catch (Exception e) {
			String msg = "request table router error";
			logger.error(msg, e);
			throw new RuntimeException("request table router error");
		}
		return routerResult;
	}

	/**
	 * 校验请求
	 * 
	 * @param sender
	 * @param receiver
	 * @param report_id
	 * @param dataSource
	 */
	private void validate(ActorRef sender, ActorRef receiver, String report_id, String dataSource) {
		if (Strings.isNullOrEmpty(report_id)) {
			throw new RuntimeException("report_id must not be blank");
		}

		if (Strings.isNullOrEmpty(dataSource)) {
			throw new RuntimeException("dataSource must not be blank");
		}
	}

	/**
	 * 根据开关， 生成cache tool<br>
	 * 如果cache关闭， 则不初始化cache tool 对象<br>
	 * 
	 * @param reportId
	 */
	private void genCacheTool(String reportId, boolean isImpala) {

		// 如果cache开关打开， 初始化一级缓存
		if (CacheCfg.getInstance().isEnableL1()) {
			cacheTool = new CacheToolChest(QueryCacheFactory.create(cfg.getCacheType()), cfg.getCacheTtlFunc());
			logger.info("L1 cache is enabled for report id " + reportId);
		} else {
			cacheTool = null;
			logger.info("L1 cache is not enabled for report id " + reportId);
		}

		// 如果二级缓存打开， 初始化二级缓存
		if (CacheCfg.getInstance().isEnableL2()) {
			cacheToolL2 = CacheToolL2.newInstance(postContext, isImpala);
			logger.info("L2 cache is enabled for report id " + reportId);
		} else {
			cacheToolL2 = null;
			logger.info("L2 cache is not enabled for report id " + reportId);
		}
	}

	/**
	 * 先查找L1 cache， 再查找L2 cache
	 * 
	 * @param reportParam
	 * @param cacheL1
	 * @param cacheL2
	 * @return
	 */
	private ReportServiceResult findCache(PostContext postContext, CacheTool cacheL1, CacheTool cacheL2) {

		if (cacheL1 != null) {
			// 先在一级缓存中查找
			ReportResult cacheResult = (ReportResult) (cacheL1.get(postContext.getReportParam(), ReportResult.class));
			if (cacheResult != null) {
				logger.info("find result in level 1 cache, key is " + postContext.getReportParam().toString());
				return new ReportServiceResult(MsgType.success, cacheResult);
			}
		}

		if (cacheL2 != null) {
			ReportResult cacheResult = (ReportResult) (cacheL2.get(postContext.getReportParam(), ReportResult.class));
			if (null != cacheResult) {
				logger.info("find result in level 2 cache, key is " + postContext.getReportParam().toStringForL2WithSort());
				return new ReportServiceResult(MsgType.success, cacheResult);
			}
		}
		return null;
	}

	@Override
	public void preStart() {
		Injector injector = Guice.createInjector(new QueryConfigModule());
		cfg = injector.getInstance(QueryConfig.class);
	}

	/**
	 * send error 消息
	 * 
	 * @param msgType
	 * @param msg
	 * @param sender
	 * @param receiver
	 */
	void sendMsg(MsgType msgType, String msg, ActorRef sender, ActorRef receiver) {
		ReportResult reportResult = new ReportResult();
		reportResult.setFlag(String.valueOf(msgType));
		reportResult.setMsg(msg);
		reportResult.setData(null);

		ReportServiceResult result = new ReportServiceResult(MsgType.fail, reportResult);
		sender.tell(result, receiver);

	}

	/**
	 * send error 消息
	 * 
	 * @param msgType
	 * @param msg
	 * @param sender
	 * @param receiver
	 */
	void sendInfoMsg(MsgType msgType, String msg, ActorRef sender, ActorRef receiver) {
		ReportResult reportResult = new ReportResult();
		reportResult.setFlag(String.valueOf(msgType));
		reportResult.setMsg(msg);
		reportResult.setData(null);

		ReportServiceResult result = new ReportServiceResult(MsgType.info, reportResult);
		sender.tell(result, receiver);

	}

	public static Props mkProps() {
		return Props.apply(QueryActor.class);
	}

	private static Logger logger = Logger.getLogger(QueryActor.class);

}
class GetResult{
	
	ReportResult getPartBySize(ReportResult ret, int page, int size, int offset) {
		ReportResult subResult = new ReportResult();
		subResult.setFlag("success");
		subResult.setMsg("ok");
		
		Entity entity = new Entity();
		ReportPage reportPage = new ReportPage();
		reportPage.setPagenumber(page);
		Object[] dimensionName = ret.getData().getData().get(0);
		List<Object[]> allData = ret.getData().getData();
		List<Object[]> partData = null;
		
		int total = (int) (ret.getData().getPage().getTotal());
		
		int start = DataRange.getStart(page, size, offset);
		int end = DataRange.getEnd(page, size, offset);
		if(start >= total || size == 0){
			partData = new ArrayList<Object[]>();
		}else if(end > total) {
			end = total;
			partData = new ArrayList<>(allData.subList(start+1, end+1));
		}else{
			partData = new ArrayList<>(allData.subList(start+1, end+1));
		}
		
		partData.add(0, dimensionName);
		entity.setData(partData);
		reportPage.setTotal(total);
		entity.setPage(reportPage);
		subResult.setData(entity);
		
		return subResult;
	}
}
