package com.yeahmobi.datasystem.query.akka.http;

import static org.junit.Assert.assertEquals;
import io.druid.query.Result;
import io.druid.query.topn.TopNResultValue;

import java.io.IOException;

import org.junit.Test;
import org.mockito.Mockito;

import com.fasterxml.jackson.core.type.TypeReference;
import com.yeahmobi.datasystem.query.process.QueryContext;
import com.yeahmobi.datasystem.query.skeleton.DataSet;
import com.yeahmobi.datasystem.query.skeleton.InMemoryDataSet;
import com.yeahmobi.datasystem.query.skeleton.PostContext;

public class DruidBodyAsyncConsumerTest {


	@Test
	public void test1() throws IOException{

		DataSet dataSetMock = Mockito.mock(InMemoryDataSet.class);
		
		QueryContext queryContext = new QueryContext();
		PostContext postContext = new PostContext(null, queryContext, null);
		queryContext.setElemType(new TypeReference<Result<TopNResultValue>>() {
		});
		
		DruidBodyAsyncConsumer con = new DruidBodyAsyncConsumer(postContext, dataSetMock, null, null, null, null, null);
		String str = "[{\"timestamp\":\"2014-08-11T08:06:00.000Z\",\"result\":[{\"click_time\":\"2014-08-11 08:25:07\",\"cr\":1.0,\"conversion\":1,\"cpc\":5.0,\"click\":1,\"cost\":5.0}";
		con.write(str.getBytes("utf-8"));
		con.tryParse();

		String str2 = "]}]";
		con.write(str2.getBytes("utf-8"));
		con.tryParse();

	}
	
	/**
	 * 先接受2.5个对象； 再接受0.5个对象； 最后接受]
	 * @throws IOException
	 */
	@Test
	public void test2() throws IOException {

		DataSet dataSetMock = Mockito.mock(InMemoryDataSet.class);
		
		QueryContext queryContext = new QueryContext();
		PostContext postContext = new PostContext(null, queryContext, null);
		queryContext.setElemType(new TypeReference<Result<TopNResultValue>>() {
		});
		
		DruidBodyAsyncConsumer con = new DruidBodyAsyncConsumer(postContext, dataSetMock, null, null, null, null, null);
		// 2.5 个object
		String str = "[{\"timestamp\":\"2014-08-11T08:06:00.000Z\",\"result\":[{\"cost\":5.0}]},{\"timestamp\":\"2014-08-11T08:06:00.000Z\",\"result\":[{\"cost\":5.0}]},{\"timestamp\":\"2014-08-11T08:06:00.000Z\"";
		con.write(str.getBytes("utf-8"));
		int num = con.tryParse();
		assertEquals(2, num);
		
		// 0.5 个object
		String str2 = ",\"result\":[{\"cost\":5.0}]}";
		con.write(str2.getBytes("utf-8"));
		num = con.tryParse();
		assertEquals(1, num);
		
		String str3 = "]";
		con.write(str3.getBytes("utf-8"));
		num = con.tryParse();
		assertEquals(0, num);

	}
}
