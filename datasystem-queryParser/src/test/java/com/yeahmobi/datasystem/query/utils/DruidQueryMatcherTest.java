package com.yeahmobi.datasystem.query.utils;

import org.junit.Test;

import com.google.common.base.Strings;

public class DruidQueryMatcherTest {

	@Test
	public void test() {
		String query ="{\"data\":[\"(doublesum(cost) as cost)\",\"(doublesum(revenue) as revenue)\"],\"filters\":\"{\\\"$and\\\":{\\\"aff_id\\\":{\\\"$match\\\":\\\"6553*\\\"},\\\"log_tye\\\":{\\\"$eq\\\":1},\\\"offer_id\\\":{\\\"$js\\\":\\\"function(x){return(x==65536)}\\\"}}}\",\"group\":[\"day\",\"country\",\"conv_ip\",\"click_ip\"],\"settings\":{\"data_source\":\"ymds_druid_datasource\",\"pagination\":{\"page\":0,\"size\":0},\"report_id\":\"f9fb3fe259c4523ce5439116556eafd1\",\"return_format\":\"json\",\"time\":{\"end\":0,\"start\":0,\"timezone\":0}},\"sort\":[]}";
    	if (!Strings.isNullOrEmpty(query)) {
    		String newquery=query.replace("\\", "").replace("\"{", "{").replace("}\",", "},");
    		newquery=newquery.replace("\"(", "(").replace(")\"", ")");
    		System.out.println(newquery);
    	 }
	}

}
