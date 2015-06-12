package com.yeahmobi.datasystem.query.process;

import static org.junit.Assert.*;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TimeSeriesMetricDimensionTranTest {

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Test
	public void test() {
		TimeSeriesMetricDimensionTran aa = new TimeSeriesMetricDimensionTran(null, null);
		Object res = "01-1";
		Object monStr = ((String) res).substring(0, 2);
    	//res = aa.getMonthName(monStr) + ((String) res).substring(2);
    	System.out.println(res);
	}

}
