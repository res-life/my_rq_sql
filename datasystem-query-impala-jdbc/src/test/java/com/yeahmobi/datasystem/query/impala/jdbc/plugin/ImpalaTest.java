package com.yeahmobi.datasystem.query.impala.jdbc.plugin;

import org.junit.BeforeClass;
import org.junit.Test;

import com.yeahmobi.datasystem.query.impala.jdbc.plugin.Impala.Router;

public class ImpalaTest {
	static Router router =null;
	@BeforeClass
	public static void doFirst(){
		router = Router.getInstance();
	}
	@Test
	public void testRouter() {
		String nodeName = router.getAvailuableNode();
		router.disableOneHour(nodeName);
		router.enableNode(nodeName);
		System.out.println(nodeName);
	}

}