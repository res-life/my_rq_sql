package com.yeahmobi.datasystem.query.akka;


import java.util.Arrays;

import jersey.repackaged.com.google.common.collect.Iterables;

import org.junit.Test;

import com.yeahmobi.datasystem.query.meta.ReportPage;
import com.yeahmobi.datasystem.query.meta.ReportResult;

public class QueryActorTest {

	@Test
	public void test() {
		GetResult ge = new GetResult();
		ReportResult ret = new ReportResult();
		ret.setFlag("success");
		ret.setMsg("ok");
		String a;
		String b;
		String c;
		String d;
		for(int i=0 ; i<100 ; i++){
			a = "a" +i;
			b = "b" +i;
			c = "c" +i;
			d = "d" +i;
			ret.append(Iterables.toArray(Iterables.concat(Arrays.asList(a,b),Arrays.asList(c,d)), String.class));
		}
		ret.setPage(new ReportPage(0, 100));
		
		for(Object[] object : ge.getPartBySize(ret, 1, 10, -1).getData().getData()){
			for (Object object2 : object) {
				System.out.print(object2);
				System.out.print("\t");
			}
			System.out.println();
		}
	}

}
