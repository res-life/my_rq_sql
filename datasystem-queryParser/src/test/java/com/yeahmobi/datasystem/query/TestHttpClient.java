package com.yeahmobi.datasystem.query;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import io.druid.data.input.Row;
import io.druid.jackson.DefaultObjectMapper;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.junit.Ignore;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.util.List;

/**
 * 测试druid请求转发到broker节点上面（目前broker节点IP地址为10.1.5.30:8080）
 * 
 * @author chenyi
 * 
 */
public class TestHttpClient {

    public static String url = "http://reportbroker-1185883114.us-east-1.elb.amazonaws.com:8080/druid/v2/?pretty";

    // public static String value =
    // "{\"queryType\" : \"groupBy\",\"dataSource\" : \"ymds_druid_datasource\",\"intervals\" : {\"type\" : \"LegacySegmentSpe\",\"intervals\" : [ \"2014-02-01T00:00:00.000Z/2014-04-01T02:00:00.000Z\" ]},\"filter\" : null,\"granularity\" : {\"type\" : \"period\",\"period\" : \"PT60M\",\"origin\" : null,\"timeZone\" : \"+08:00\"},\"dimensions\" : [ {\"type\" : \"default\",\"dimension\" : \"offer_id\",\"outputName\" :\"offer_id\"} ],\"aggregations\" : [ {\"type\" :\"longSum\",\"name\" : \"sum(conversion)\",\"fieldName\" : \"conversion\"}, {\"type\" : \"longSum\",\"name\" : \"sum(click)\",\"fieldName\" : \"click\"} ],\"postAggregations\" : [ {\"type\" : \"arithmetic\",\"name\" : \"(sum(conversion)/sum(click))\",\"fn\" : \"/\",\"fields\" : [ {\"type\" : \"fieldAccess\",\"name\" : \"sum(conversion)\",\"fieldName\" : \"sum(conversion)\"}, {\"type\" : \"fieldAccess\",\"name\" : \"sum(click)\",\"fieldName\" : \"sum(click)\"} ]}, {\"type\" : \"fieldAccess\",\"name\" : \"cr\",\"fieldName\" : \"(sum(conversion)/sum(click))\"} ],\"having\" : null,\"orderBy\" : {\"type\" : \"default\",\"columns\" : [ ],\"limit\" : 1000},\"context\" : null}";

    // public static String value =
    // "{\"queryType\" : \"groupBy\",\"dataSource\" : \"ymds_druid_datasource\",\"intervals\" : {\"type\" : \"LegacySegmentSpec\",\"intervals\" : [ \"2014-02-01T00:00:00.000Z/2014-02-05T01:00:00.000Z\" ]},\"filter\" : null,\"granularity\" : {\"type\" : \"period\",\"period\" : \"PT60M\",\"origin\" : null,\"timeZone\" : \"+08:00\"},\"dimensions\" : [ {\"type\" : \"default\",\"dimension\" : \"offer_id\",\"outputName\" : \"offer_id\"} ],\"aggregations\" : [ {\"type\" : \"longSum\",\"name\" : \"sum(click)\",\"fieldName\" : \"click\"}, {\"type\" : \"longSum\",\"name\" : \"sum(conversion)\",\"fieldName\" : \"conversion\"}, {\"type\" : \"longSum\",\"name\" : \"sum(revenue)\",\"fieldName\" : \"revenue\"}, {\"type\" : \"longSum\",\"name\" : \"sum(cost)\",\"fieldName\" : \"cost\"} ],\"postAggregations\" : [ {\"type\" : \"fieldAccess\",\"name\" : \"sum(click)\",\"fieldName\" : \"sum(click)\"}, {\"type\" : \"fieldAccess\",\"name\" : \"sum(conversion)\",\"fieldName\" : \"sum(conversion)\"}, {\"type\" : \"arithmetic\",\"name\" : \"(sum(revenue)-sum(cost))\",\"fn\" : \"-\",\"fields\" : [ {\"type\" : \"fieldAccess\",\"name\" : \"sum(revenue)\",\"fieldName\" : \"sum(revenue)\"}, {\"type\" : \"fieldAccess\",\"name\" : \"sum(cost)\",\"fieldName\" : \"sum(cost)\"} ]}, {\"type\" : \"arithmetic\",\"name\" : \"(sum(conversion)/sum(click))\",\"fn\" : \"/\",\"fields\" : [ {\"type\" : \"fieldAccess\",\"name\" : \"sum(conversion)\",\"fieldName\" : \"sum(conversion)\"}, {\"type\" : \"fieldAccess\",\"name\" : \"sum(click)\",\"fieldName\" : \"sum(click)\"} ]}, {\"type\" : \"arithmetic\",\"name\" : \"(sum(revenue)/sum(click))\",\"fn\" : \"/\",\"fields\" : [ {\"type\" : \"fieldAccess\",\"name\" : \"sum(revenue)\",\"fieldName\" : \"sum(revenue)\"}, {\"type\" : \"fieldAccess\",\"name\" : \"sum(click)\",\"fieldName\" : \"sum(click)\"} ]}, {\"type\" : \"fieldAccess\",\"name\" : \"click\",\"fieldName\" : \"sum(click)\"}, {\"type\" : \"fieldAccess\",\"name\" : \"conversion\",\"fieldName\" : \"sum(conversion)\"}, {\"type\" : \"fieldAccess\",\"name\" : \"profit\",\"fieldName\" : \"(sum(revenue)-sum(cost))\"}, {\"type\" : \"fieldAccess\",\"name\" : \"cr\",\"fieldName\" : \"(sum(conversion)/sum(click))\"}, {\"type\" : \"fieldAccess\",\"name\" : \"rpc\",\"fieldName\" : \"(sum(revenue)/sum(click))\"} ],\"having\" : null,\"orderBy\" : {\"type\" : \"default\",\"columns\" : [ ],\"limit\" : 1000},\"context\" : null}";
    // public static String value =
    // "{\"queryType\" : \"groupBy\", \"dataSource\" : \"ymds_druid_datasource\", \"intervals\" : {\"type\" : \"LegacySegmentSpec\", \"intervals\" : [ \"2014-01-31T16:00:00.000Z/2014-02-04T00:00:00.000Z\" ] }, \"filter\" : null, \"granularity\" : {\"type\" : \"period\", \"period\" : \"P1D\", \"origin\" : null, \"timeZone\" : \"+08:00\"}, \"dimensions\" : [ {\"type\" : \"default\", \"dimension\" : \"offer_id\", \"outputName\" : \"offer_id\"}, {\"type\" : \"default\", \"dimension\" : \"aff_id\", \"outputName\" : \"aff_id\"}, {\"type\" : \"default\", \"dimension\" : \"aff_manager\", \"outputName\" : \"aff_manager\"} ], \"aggregations\" : [ {\"type\" : \"longSum\", \"name\" : \"sum(click)\", \"fieldName\" : \"click\"}, {\"type\" : \"longSum\", \"name\" : \"sum(conversion)\", \"fieldName\" : \"conversion\"}, {\"type\" : \"longSum\", \"name\" : \"sum(cost)\", \"fieldName\" : \"cost\"} ], \"postAggregations\" : [ {\"type\" : \"fieldAccess\", \"name\" : \"sum(click)\", \"fieldName\" : \"sum(click)\"}, {\"type\" : \"fieldAccess\", \"name\" : \"sum(conversion)\", \"fieldName\" : \"sum(conversion)\"}, {\"type\" : \"fieldAccess\", \"name\" : \"sum(cost)\", \"fieldName\" : \"sum(cost)\"}, {\"type\" : \"arithmetic\", \"name\" : \"(sum(conversion)/sum(click))\", \"fn\" : \"/\", \"fields\" : [ {\"type\" : \"fieldAccess\", \"name\" : \"sum(conversion)\", \"fieldName\" : \"sum(conversion)\"}, {\"type\" : \"fieldAccess\", \"name\" : \"sum(click)\", \"fieldName\" : \"sum(click)\"} ] }, {\"type\" : \"fieldAccess\", \"name\" : \"click\", \"fieldName\" : \"sum(click)\"}, {\"type\" : \"fieldAccess\", \"name\" : \"conversion\", \"fieldName\" : \"sum(conversion)\"}, {\"type\" : \"fieldAccess\", \"name\" : \"cost\", \"fieldName\" : \"sum(cost)\"}, {\"type\" : \"fieldAccess\", \"name\" : \"cr\", \"fieldName\" : \"(sum(conversion)/sum(click))\"} ], \"having\" : null, \"orderBy\" : {\"type\" : \"default\", \"columns\" : [ ], \"limit\" : 1000 }, \"context\" : null }";

    public static String value = "{\"queryType\" : \"groupBy\", \"dataSource\" : \"ymds_druid_datasource\", \"intervals\" : {\"type\" : \"LegacySegmentSpec\", \"intervals\" : [ \"2014-04-23T04:00:00.000Z/2014-04-24T00:00:00.000Z\" ] }, \"filter\" : null, \"granularity\" : {\"type\" : \"period\", \"period\" : \"PT1H\", \"origin\" : null, \"timeZone\" : \"+08:00\"}, \"dimensions\" : [ {\"type\" : \"default\", \"dimension\" : \"offer_id\", \"outputName\" : \"offer_id\"} ], \"aggregations\" : [ {\"type\" : \"longSum\", \"name\" : \"sum(click)\", \"fieldName\" : \"click\"}, {\"type\" : \"longSum\", \"name\" : \"sum(conversion)\", \"fieldName\" : \"conversion\"} ], \"postAggregations\" : [ {\"type\" : \"fieldAccess\", \"name\" : \"sum(click)\", \"fieldName\" : \"sum(click)\"}, {\"type\" : \"arithmetic\", \"name\" : \"(sum(conversion)/sum(click))\", \"fn\" : \"/\", \"fields\" : [ {\"type\" : \"fieldAccess\", \"name\" : \"sum(conversion)\", \"fieldName\" : \"sum(conversion)\"}, {\"type\" : \"fieldAccess\", \"name\" : \"sum(click)\", \"fieldName\" : \"sum(click)\"} ] }, {\"type\" : \"fieldAccess\", \"name\" : \"click\", \"fieldName\" : \"sum(click)\"}, {\"type\" : \"fieldAccess\", \"name\" : \"cr\", \"fieldName\" : \"(sum(conversion)/sum(click))\"} ], \"having\" : null, \"orderBy\" : {\"type\" : \"default\", \"columns\" : [ ], \"limit\" : 1000 }, \"context\" : null }";

    // "{\"queryType\" : \"groupBy\",\"dataSource\" : \"ymds_druid_datasource\",\"intervals\" : {\"intervals\" : [ \"2014-02-01T00:00:00.000Z/2014-04-01T02:00:00.000Z\" ]},\"filter\" : null,\"granularity\" : {\"type\" : \"period\",\"period\" : \"PT60M\",\"origin\" : null,\"timeZone\" : \"+08:00\"},\"dimensions\" : [ {\"type\" : \"default\",\"dimension\" : \"offer_id\",\"outputName\" :\"offer_id\"} ],\"aggregations\" : [ {\"type\" :\"longSum\",\"name\" : \"sum(conversion)\",\"fieldName\" : \"conversion\"}, {\"type\" : \"longSum\",\"name\" : \"sum(click)\",\"fieldName\" : \"click\"} ],\"postAggregations\" : [ {\"type\" : \"arithmetic\",\"name\" : \"(sum(conversion)/sum(click))\",\"fn\" : \"/\",\"fields\" : [ {\"type\" : \"fieldAccess\",\"name\" : \"sum(conversion)\",\"fieldName\" : \"sum(conversion)\"}, {\"type\" : \"fieldAccess\",\"name\" : \"sum(click)\",\"fieldName\" : \"sum(click)\"} ]}, {\"type\" : \"fieldAccess\",\"name\" : \"cr\",\"fieldName\" : \"(sum(conversion)/sum(click))\"} ],\"having\" : null,\"orderBy\" : {\"type\" : \"default\",\"columns\" : [ ],\"limit\" : 1000},\"context\" : null}";

    public void testInit() throws ClientProtocolException, IOException {
        DefaultHttpClient client = new DefaultHttpClient();
        HttpPost httpPost = new HttpPost(url);
        httpPost.setHeader("content-type", "application/json");
        httpPost.setEntity(new StringEntity(value, "UTF-8"));
        long start = System.currentTimeMillis();
        HttpResponse response = client.execute(httpPost);
        System.out.println(response.getStatusLine().getStatusCode());
        long end = System.currentTimeMillis();
        System.out.println((end - start) + "ms");
        BufferedReader in = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));
        String line = null;
        while ((line = in.readLine()) != null) {
            System.out.println(line);
        }
        // byte[] b = new byte[(int)
        // response.getEntity().getContentLength()];
        // in.read(b);
        // System.out.println(new String(b,"UTF-8"));

    }

    @Ignore
    public void testRequest() throws Exception {
        URL url = new URL(TestHttpClient.url);
        URLConnection urlConnection = url.openConnection();
        urlConnection.addRequestProperty("content-type", "application/json");
        urlConnection.setDoOutput(true);
        urlConnection.getOutputStream().write(value.getBytes(Charsets.UTF_8));
        BufferedReader stdInput = new BufferedReader(new InputStreamReader(urlConnection.getInputStream()));
        ObjectMapper objectMapper = new DefaultObjectMapper();
        Object res = objectMapper.readValue(stdInput, new TypeReference<List<Row>>(){});
        System.out.println("========================");
        System.out.println(res);
    }

    public static void main(String[] args) throws Exception {
        TestHttpClient client = new TestHttpClient();
        client.testRequest();
    }
}
