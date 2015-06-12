package com.yeahmobi.datasystem.query.impala;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.yeahmobi.datasystem.query.meta.GlobalCfg;

public class ImpalaThreadPool {

	private static ThreadPoolExecutor threadPool = null;
	static {
		threadPool = new ThreadPoolExecutor(GlobalCfg.getInstance().getImpalaCorePoolSize(), GlobalCfg.getInstance()
				.getImpalaMaximumPoolSize(), 20L, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>());
	}

	private ImpalaThreadPool() {
	}

	public static ThreadPoolExecutor get() {
		return threadPool;
	}
}
