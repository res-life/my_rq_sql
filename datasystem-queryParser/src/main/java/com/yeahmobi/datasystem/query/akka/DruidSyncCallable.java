package com.yeahmobi.datasystem.query.akka;

import java.util.concurrent.Callable;

import com.ning.http.client.AsyncHandler;
import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.ListenableFuture;
import com.yeahmobi.datasystem.query.exception.ReportRuntimeException;
import com.yeahmobi.datasystem.query.skeleton.PostProcess;

public class DruidSyncCallable implements Callable<Boolean> {

	private String queryStr;
	private AsyncHttpClient client;
	private PostProcess postProcess;
	public DruidSyncCallable(String queryStr, AsyncHttpClient client, PostProcess postProcess){
		this.queryStr = queryStr;
		this.client = client;
		this.postProcess = postProcess;
	}
	@Override
	public Boolean call() throws Exception {
		
		
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

		return null;
	}

}
