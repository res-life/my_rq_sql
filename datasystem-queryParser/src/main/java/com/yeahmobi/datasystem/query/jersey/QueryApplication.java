package com.yeahmobi.datasystem.query.jersey;

/**
 * Created by yangxu on 5/5/14.
 */

import java.util.HashSet;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.ws.rs.core.Application;

import org.apache.log4j.Logger;
import org.glassfish.hk2.api.DynamicConfiguration;
import org.glassfish.hk2.api.ServiceLocator;
import org.glassfish.jersey.internal.inject.Injections;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.rmrodrigues.pf4j.web.PluginManagerHolder;

import scala.concurrent.duration.Duration;
import akka.actor.ActorSystem;
import akka.routing.DefaultResizer;
import akka.routing.Resizer;
import akka.routing.RoundRobinRouter;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.jaxrs.json.JacksonJsonProvider;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.yeahmobi.datasystem.query.akka.QueryActor;
import com.yeahmobi.datasystem.query.akka.QueryConfig;
import com.yeahmobi.datasystem.query.akka.QueryConfigModule;
import com.yeahmobi.datasystem.query.akka.cache.CacheTool;
import com.yeahmobi.datasystem.query.akka.cache.QueryCacheFactory;
import com.yeahmobi.datasystem.query.akka.cache.XchangeRateCacheToolChest;
import com.yeahmobi.datasystem.query.assist.XchangeRateCacheHelper;
import com.yeahmobi.datasystem.query.config.ConfigManager;
import com.yeahmobi.datasystem.query.queue.QueryExecuteThreadPool;
import com.yeahmobi.datasystem.query.skeleton.DataSourceViews;
import com.yeahmobi.datasystem.query.skeleton.Formatters;

public class QueryApplication extends Application {

	private static Logger logger = Logger.getLogger(QueryApplication.class);

	private ActorSystem system;
	static QueryConfig cfg;

	@Inject
	public QueryApplication(ServiceLocator serviceLocator) {

		Injector injector = Guice.createInjector(new QueryConfigModule());
		cfg = injector.getInstance(QueryConfig.class);

		system = ActorSystem.create("QuerySystem");
		Resizer resizer = new DefaultResizer(cfg.getLowerBound(),
				cfg.getUpperBound());
		
		// assign QueryRouter's actor as QueryActor class
		system.actorOf(
				QueryActor.mkProps().withRouter(new RoundRobinRouter(resizer)),
				"QueryRouter");

		DynamicConfiguration dc = Injections.getConfiguration(serviceLocator);
		Injections.addBinding(Injections.newBinder(system)
				.to(ActorSystem.class), dc);
		Injections.addBinding(Injections.newBinder(cfg).to(QueryConfig.class),
				dc);
		dc.commit();

	}

	@PostConstruct
	private void init() {

		// init broker context

		ConfigManager.getInstance().init();

		
		// init query queue timer
		new Timer().schedule(new TimerTask() {
			public void run() {
				QueryExecuteThreadPool.initCustomerPool(); // 定时唤醒 查询执行线程池
			}
		}, cfg.getDelay(), cfg.getPeriod());
		
		if (XchangeRateCacheHelper.xchangeIsEnabled()) {
		    // 首次启动,更新汇率转换缓存
		    CacheTool cacheTool = new XchangeRateCacheToolChest(QueryCacheFactory.create(cfg.getXchangeCacheType()), cfg.getXchangeCacheTtlFunc());
		    if (XchangeRateCacheHelper.updateCache(cacheTool)) {
		        logger.info("Updated Cache : XchangeRateCacheType[" + cfg.getXchangeCacheType() +  "],XchangeRateCacheTTL[" + cfg.getXchangeCacheTtlFunc().apply() + "].");
		    }
		}

		// load the plug-ins and get all the data source plug-ins into map
		startPlugins();

	}

	private void startPlugins() {
		PluginManagerHolder.getPluginManager().startPlugins();
		DataSourceViews.init();
		Formatters.init();
	}

	@PreDestroy
	private void shutdown() {
		
		system.shutdown();
		system.awaitTermination(Duration.create(15, TimeUnit.SECONDS));
	}

	/**
	 * return the web services classes
	 */
	@Override
	public Set<Class<?>> getClasses() {
		Set<Class<?>> classSet = new HashSet<Class<?>>();
		classSet.add(ReportService.class);
		classSet.add(QueryService.class);
		classSet.add(OkService.class);
		classSet.add(LogLevelService.class);
		classSet.add(InputCheckService.class);
		classSet.add(PluginService.class);
		classSet.add(CacheCfgService.class);
		classSet.add(GlobalCfgService.class);
		classSet.add(MetricDimentionResetService.class);
		classSet.add(DimensionService.class);
		classSet.add(MetricService.class);
		classSet.add(ImpalaCfgService.class);
		
		return classSet;
	}

	@Override
	public Set<Object> getSingletons() {
		Set<Object> s = new HashSet<Object>();

		s.add(JacksonFeature.class);

		return s;
	}
}
