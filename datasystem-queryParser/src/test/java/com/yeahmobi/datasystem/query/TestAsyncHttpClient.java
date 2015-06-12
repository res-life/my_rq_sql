package com.yeahmobi.datasystem.query;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.datatype.joda.JodaModule;
import com.google.common.base.Stopwatch;
import com.google.common.base.Strings;
import com.google.gson.Gson;
import com.metamx.common.ISE;
import com.ning.http.client.*;
import com.yeahmobi.datasystem.query.antlr4.DruidReportParser;
import com.yeahmobi.datasystem.query.antlr4.WarpInterpreter;
import com.yeahmobi.datasystem.query.exception.FileErrorListener;
import com.yeahmobi.datasystem.query.exception.ReportParserException;
import com.yeahmobi.datasystem.query.meta.ReportResult;
import com.yeahmobi.datasystem.query.process.*;
import io.druid.data.input.MapBasedRow;
import io.druid.jackson.DefaultObjectMapper;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.junit.Ignore;

import java.io.*;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Created by yangxu on 13-11-30.
 */
public class TestAsyncHttpClient {
    private static final Logger logger = Logger.getLogger(TestAsyncHttpClient.class);

    private static String queryParams = "{\"settings\":{\"time\":{\"start\":1391447700,\"end\":1392272126,\"timezone\":0},\"data_source\":\"ymds_druid_datasource\",\"pagination\":{\"size\":50,\"page\":0}},\"group\":[\"offer_id\",\"aff_id\",\"week\"],\"data\":[\"click\",\"conversion\",\"cr\",\"cpc\",\"rpc\"],"
            + "\"filters\":{" +
            "\"$and\":{" +
            "\"offer_id\":{\"$in\":[14044]}" +
//            "\"aff_id\":{\"$in\":[13208]}" +
            "}},"
            + "\"sort\":[],\"currency\":USD}";

    @Ignore
    public void test() {
        query(queryParams);
    }

    String query(String data) {
        String queryData = data;
        if (Strings.isNullOrEmpty(queryData)) {
            return "report_param must not be blank";
        }

        long id = DateTime.now().getMillis();
        logger.info("[phase:in][query=" + queryData + "][id=" + id + "]");

        Stopwatch stopwatch = Stopwatch.createStarted();
        String queryParams = PreProcessor.process(queryData);// 预处理阶段
        logger.info("[phase:pre-process][query=" + id + "] --> " + stopwatch.elapsed(TimeUnit.MILLISECONDS));

        if (logger.isDebugEnabled()) {
            logger.debug(queryParams);
        }
        DruidReportParser parser = null;// 语法解析
        try {
            parser = WarpInterpreter.convert(queryParams, "ymds_druid_datasource",new FileErrorListener());
        } catch (ReportParserException e) {
            logger.error(e.getMessage(), e);
            return e.getMessage();
        }

        logger.info("[phase:parse][query=" + id + "] --> " + stopwatch.elapsed(TimeUnit.MILLISECONDS));
        QueryContext ctx = null;
        try {
            // ctx = QueryFactory.create(parser);// 创建对应的druid查询对象
            // add by martin 20140701 start
            ctx = QueryFactory.create(parser, queryData);// 创建对应的druid查询对象
            // add by martin 20140701 end
        } catch (ISE e) {
            logger.error(e.getMessage(), e);
            return e.getMessage();
        }
        logger.info("[phase:build][query=" + id + "] --> " + stopwatch.elapsed(TimeUnit.MILLISECONDS));

        String queryStr;
        ReportResult reportResult = null;

        ObjectMapper objectMapper = new DefaultObjectMapper();
        ObjectWriter jsonWriter = objectMapper.writerWithDefaultPrettyPrinter();
        try {
            queryStr = jsonWriter.writeValueAsString(ctx.getQuery());// 将druid查询对象转换成string
            if (logger.isDebugEnabled()) {
                logger.debug(parser.orderBy);
                logger.debug(queryStr);
            }


            AsyncHttpClientConfig.Builder builder = new AsyncHttpClientConfig.Builder();
            builder.setCompressionEnabled(true)
                    .setAllowPoolingConnection(true)
                    .setRequestTimeoutInMs(6000000);

            AsyncHttpClient client = new AsyncHttpClient(builder.build());

            client.preparePost("http://10.1.5.30:8080/druid/v2/")
                    .addHeader("content-type", "application/json")
                    .setBody(queryStr)
                    .execute(new DruidAsyncHandler(objectMapper, ctx, parser)).get();

            client.closeAsynchronously();

        } catch (JsonProcessingException e) {
            logger.error("", e);
        } catch (MalformedURLException e) {
            logger.error("", e);
        } catch (IOException e) {
            logger.error("", e);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }

        System.err.println("done");
        return  "done";
    }
}


class DruidBodyConsumer {

    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    List<MapBasedRow> rows = new ArrayList<MapBasedRow>();

    JsonFactory jsonFactory = new JsonFactory();
    ObjectMapper mapper = new ObjectMapper().registerModule(new JodaModule());
    int offset = 0;

    void write(byte[] bytes) throws IOException {
        stream.write(bytes);
    }

    int tryParse() throws IOException {
        stream.flush();
        if (offset >= stream.size()) return 0;
        JsonParser jsonParser = jsonFactory.createParser(stream.toByteArray(), offset, stream.size() - offset);
        if (offset == 0) {
            jsonParser.nextToken(); // skip '['
            ++offset;
        }

        int n = 0;
        int bytes = 0;
        while (true) {
            try {
                JsonToken token = jsonParser.nextToken();
                if (token == JsonToken.START_OBJECT) {
                    bytes -= jsonParser.getCurrentLocation().getColumnNr();
                    MapBasedRow row = mapper.readValue(jsonParser, MapBasedRow.class);
                    rows.add(row);
                    bytes += (jsonParser.getCurrentLocation().getColumnNr() + 2); // '{' '}' ','
                    ++n;
                } else {
                    break;
                }
            } catch (JsonProcessingException e) {
                break;
            }
        }
        offset += bytes;

        return n;
    }
}

class DruidAsyncHandler implements AsyncHandler<Response> {
    @Override
    public void onThrowable(Throwable throwable) {

    }

    private final Response.ResponseBuilder builder =
            new Response.ResponseBuilder();

    final DruidBodyConsumer consumer = new DruidBodyConsumer();

    ObjectMapper objectMapper;
    QueryContext ctx;
    DruidReportParser parser;

    DruidAsyncHandler(ObjectMapper objectMapper, QueryContext ctx) {
        this.objectMapper = objectMapper;
        this.ctx = ctx;
    }

    DruidAsyncHandler(ObjectMapper objectMapper, QueryContext ctx, DruidReportParser parser) {
        this.objectMapper = objectMapper;
        this.ctx = ctx;
        this.parser = parser;
    }

    public STATE onBodyPartReceived(final HttpResponseBodyPart content)
            throws Exception {

        consumer.stream.write(content.getBodyPartBytes());
        consumer.tryParse();

        builder.accumulate(content);
        return STATE.CONTINUE;
    }

    public STATE onStatusReceived(final HttpResponseStatus status)
            throws Exception {
        System.err.println(status.getStatusText());
        builder.accumulate(status);
        return STATE.CONTINUE;
    }

    public STATE onHeadersReceived(final HttpResponseHeaders headers)
            throws Exception {
        builder.accumulate(headers);
        return STATE.CONTINUE;
    }

    public Response onCompleted() throws Exception {
          BufferedReader stdInput = new BufferedReader(new InputStreamReader(
                  new ByteArrayInputStream(
                          builder.build().getResponseBodyAsBytes()
                  )));

//        Object res = objectMapper.readValue(stdInput, ctx.getTypeRef());

        Object res = consumer.rows;

        PostProcessor postProcessor = PostProcessorFactory.create(ctx.getQueryType(), parser);
        // 后处理阶段
        ReportResult reportResult = postProcessor.process((List<?>)res);


        System.err.println(new Gson().toJson(reportResult));
        System.err.println(builder.build().getResponseBody());

        return builder.build();
    }
}

