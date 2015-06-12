package com.yeahmobi.datasystem.query.reportrequest;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import junit.framework.TestCase;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.google.common.collect.Lists;
import com.yeahmobi.datasystem.query.reportrequest.Settings.Pagination;
import com.yeahmobi.datasystem.query.reportrequest.Settings.Time;

/**
 * Unit test for simple App.
 */
public class ReportQuestTest extends TestCase {
	/**
	 * Create the test case
	 * 
	 * @param testName
	 *            name of the test case
	 */
	public ReportQuestTest(String testName) {
		super(testName);
	}

	public void testToString() {
		// group
		List<String> group = new ArrayList<String>();
		group.add("offer_id");
		group.add("aff_id");
		group.add("hour");
		group.add("os_id");

		// data
		List<String> data = new ArrayList<String>();
		data.add("click");
		data.add("cr");
		data.add("cost");

		// setting
		Settings settings = new Settings();
		settings.setReport_id("f9fb3fe259c4523ce5439116556eafd1");
		settings.setData_source("ymds_druid_datasource");
		settings.setPagination(new Pagination(100, 1));
		settings.setTime(new Time(1397934000000L,1397934000000L,8));
		settings.setReturn_format("file");


		ReportParam rp = new ReportParam();
		rp.setSettings(settings);
		rp.setCurrency_type("RMB");
		rp.setGroup(group);
		rp.setFilters("\"$and\":{\"log_tye\":{\"$eq\":1},\"offer_id\":{\"$js\":\"function(x){return(x==65536)}\"},\"aff_id\":{\"$match\":\"6553*\"}}}");
		rp.setData(data);
		
		TopN topn=new TopN();
		topn.setMetricvalue("click");
		topn.setThreshold(4);
		rp.setTopn(topn);
		ExtractDimension extractDimension = new ExtractDimension();
		extractDimension.setExtractDimension("click_ip");
		extractDimension.setJsFunction("\"function(str){ return str.substring(0,str.lastIndexOf('.'));}\"");		
		rp.setExtract(Lists.newArrayList(extractDimension));
		
		String json = ReportParamFactory.toString(rp);
		System.out.println(json);
		
		ReportParam obj=ReportParamFactory.toObject(json);
		System.out.println(obj.getSettings());
	}

	public void testToObject() {
		String params = "{\"settings\":{\"report_id\":\"f9fb3fe259c4523ce5439116556eafd1\",\"return_format\":\"json\",\"data_source\":\"ymds_druid_datasource\",\"time\":{\"start\":1402484400,\"end\":1402488000,\"timezone\":0},\"pagination\":{\"size\":10000,\"page\":0}},\"group\":[\"day\",\"adv_sub2\",\"adv_sub3\",\"adv_sub4\",\"adv_sub5\",\"adv_sub6\",\"adv_sub7\",\"adv_sub8\",\"ref_track_site\",\"device_brand\",\"device_model\",\"platform_id\",\"device_os\",\"user_agent\",\"click_time\",\"conv_time\",\"time_diff\",\"aff_id\",\"aff_manager\",\"aff_sub1\",\"aff_sub2\",\"aff_sub3\",\"aff_sub4\",\"aff_sub5\",\"aff_sub6\",\"aff_sub7\",\"aff_sub8\",\"adv_id\",\"adv_manager\",\"adv_sub1\",\"offer_id\",\"transaction_id\",\"ref_track\",\"browser\",\"country\",\"conv_ip\",\"click_ip\"],\"data\":[\"cost\",\"revenue\"],\"filters\":{\"$and\":{\"log_tye\":{\"$eq\":1},\"offer_id\":{\"$js\":\"function(x){return(x==65536)}\"},\"aff_id\":{\"$match\":\"6553*\"}}}}";
  //	String params="{\"settings\":{\"time\":{\"start\":1404172800,\"end\":1412121600,\"timezone\":0},\"return_format\":\"json\",\"report_id\":\"232sds3232\",\"data_source\":\"contrack_druid_datasource_ds\",\"pagination\":{\"size\":1000000,\"page\":0}},\"group\":[\"offer_id\"],\"data\":[\"clicks\",\"outs\"],\"sort\":[]}";
		ReportParam rp = ReportParamFactory.toObject(params);
		System.out.println(rp.getFilters());
	}
	
	public void getFilters() throws Exception{
//		String params="{\"settings\":{\"time\":{\"start\":1404172800,\"end\":1412121600,\"timezone\":0},\"return_format\":\"json\",\"report_id\":\"232sds3232\",\"data_source\":\"ymds_druid_datasource\",\"pagination\":{\"size\":1000000,\"page\":0}},\"group\":[\"offer_id\",\"year\",\"month\",\"week\",\"day\",\"hour\"],\"data\":[\"click\",\"conversion\"],\"filters\":{\"$and\":{hello+world}},\"sort\":[]}";
		String params="{\"settings\":{\"time\":{\"start\":1404172800,\"end\":1412121600,\"timezone\":0},\"return_format\":\"json\",\"report_id\":\"232sds3232\",\"data_source\":\"contrack_druid_datasource_ds\",\"pagination\":{\"size\":1000000,\"page\":0}},\"group\":[\"offer_id\"],\"data\":[\"clicks\",\"outs\"],\"filters\":{\"$and\":{\"clicks\":{\"$gte\":517},\"clicks\":{\"$match\":\"sdgfs\"}}},\"sort\":[]}";
//		String params = "{\"settings\":{\"report_id\":\"f9fb3fe259c4523ce5439116556eafd1\",\"return_format\":\"json\",\"data_source\":\"ymds_druid_datasource\",\"time\":{\"start\":1402484400,\"end\":1402488000,\"timezone\":0},\"pagination\":{\"size\":10000,\"page\":0}},\"group\":[\"day\",\"adv_sub2\",\"adv_sub3\",\"adv_sub4\",\"adv_sub5\",\"adv_sub6\",\"adv_sub7\",\"adv_sub8\",\"ref_track_site\",\"device_brand\",\"device_model\",\"platform_id\",\"device_os\",\"user_agent\",\"click_time\",\"conv_time\",\"time_diff\",\"aff_id\",\"aff_manager\",\"aff_sub1\",\"aff_sub2\",\"aff_sub3\",\"aff_sub4\",\"aff_sub5\",\"aff_sub6\",\"aff_sub7\",\"aff_sub8\",\"adv_id\",\"adv_manager\",\"adv_sub1\",\"offer_id\",\"transaction_id\",\"ref_track\",\"browser\",\"country\",\"conv_ip\",\"click_ip\"],\"data\":[\"cost\",\"revenue\"],\"filters\":{\"$and\":{\"log_tye\":{\"$eq\":1},\"offer_id\":{\"$js\":\"function(x){return(x==65536)}\"},\"aff_id\":{\"$match\":\"6553*\"}}}}";
//		String params="{\"settings\":{\"time\":{\"start\":1404172800,\"end\":1412121600,\"timezone\":0},\"return_format\":\"json\",\"report_id\":\"232sds3232\",\"data_source\":\"contrack_druid_datasource_ds\",\"pagination\":{\"size\":1000000,\"page\":0}},\"group\":[\"offer_id\"],\"data\":[\"clicks\",\"outs\"],\"filters\":{\"$and\":{\"clicks\":{\"$gte\":517},\"clicks\":{\"$lt\":100000}}},\"sort\":[]}";
//		String params="{\"data\":[\"clicks\",\"convs\",\"cr\",\"epc\",\"income\",\"cost\",\"net\",\"roi\"],\"filters\":{\"$and\":{\"campaign_id\":{\"$eq\":\"p_2225\"},\"clicks\":{\"$gt\":20}}},\"group\":[\"offer_id\",\"sub1\"],\"settings\":{\"time\":{\"start\":1413072000,\"end\":1413331200,\"timezone\":5.0},\"data_source\":\"contrack_druid_datasource_ds\",\"report_id\":\"22223cf19d99-11ce-4f63-9a90-bc3065c22f40\",\"pagination\":{\"size\":50,\"page\":0}},\"sort\":[{\"orderBy\":\"clicks\",\"order\":-1}]}";
		
		ReportParamFactory rf = new ReportParamFactory();
		Method method = rf.getClass().getDeclaredMethod("getFilters",String.class);
		method.setAccessible(true);
		method.invoke(rf, params);
		
		
		/*int findex=params.indexOf("\"filters\":");
		int a = 0;
		int b = 0;
		int beginIndex = findex + 10;
		
		JsonFactory jsonFactory = new JsonFactory();
		String str = params.substring(beginIndex);
		Reader reader = new StringReader(str);
		JsonParser jsonParser = jsonFactory.createParser(reader);
		JsonToken aToken = jsonParser.nextToken();
		if(aToken == JsonToken.START_OBJECT){
			a = (int) jsonParser.getCurrentLocation().getCharOffset();
			System.out.println(a);
			jsonParser.skipChildren();
			b = (int) jsonParser.getCurrentLocation().getCharOffset();
			System.out.println(b);
		}
		
		System.err.println(str.substring(a, b+1));*/
		
		/*do{
			a = params.indexOf("{",beginIndex);
			b = params.indexOf("}",beginIndex);
			if(a > b || a == -1){
				sum = sum-1;
				beginIndex = b;
			}else{
				sum = sum+1;
				beginIndex = a;
			}
			++beginIndex;
		}while(sum > 0);*/
		
//		String	filters=params.substring(findex+10, b+1);
		
//		System.out.println(filters);
	 }
}
