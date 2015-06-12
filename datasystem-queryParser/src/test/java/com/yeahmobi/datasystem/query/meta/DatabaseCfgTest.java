package com.yeahmobi.datasystem.query.meta;

import org.junit.Assert;
import org.junit.Test;

public class DatabaseCfgTest {

	@Test
	public void test() {

		String host = "127.0.0.1";
		int port = 9092;
		Assert.assertEquals("host is incorrect", host, DatabaseCfg
				.getInstance().getHost());
		Assert.assertEquals("host is incorrect", port, DatabaseCfg
				.getInstance().getPort());
	}

}
