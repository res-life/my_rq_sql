package com.yeahmobi.datasystem.query.akka;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.yeahmobi.datasystem.query.meta.GlobalCfg;

public class DruidSyncThreadPool {

	static final Injector injector = Guice.createInjector(new QueryConfigModule());
	private final static Logger logger = Logger.getLogger(DruidSyncThreadPool.class);
	
	private static ThreadPoolExecutor threadPool = null;
	static {
		QueryConfig cfg = injector.getInstance(QueryConfig.class);
		
		int queueSize = cfg.getRequestDruidSyncBlockingQueueSize();
		logger.info("druid sync request blocking queue size is " + queueSize);
		
		threadPool = new ThreadPoolExecutor(GlobalCfg.getInstance().getDruidSyncRequestCorePoolSize(), GlobalCfg
				.getInstance().getDruidSyncRequestMaxPoolSize(), 20L, TimeUnit.SECONDS,
				new ArrayBlockingQueue<Runnable>(queueSize));
	}

	private DruidSyncThreadPool() {
	}

	public static ThreadPoolExecutor get() {
		return threadPool;
	}
}
