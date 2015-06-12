package com.yeahmobi.datasystem.query.queue;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.URLEncoder;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import com.google.common.base.Strings;
import com.ning.http.client.AsyncHandler;
import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.AsyncHttpClientConfig;
import com.yeahmobi.datasystem.query.assist.NetService;
import com.yeahmobi.datasystem.query.aws.AWSFileStore;
import com.yeahmobi.datasystem.query.config.ConfigKeyConst;
import com.yeahmobi.datasystem.query.config.ConfigLoader;
import com.yeahmobi.datasystem.query.impala.ImpalaCallable;
import com.yeahmobi.datasystem.query.meta.QueryEntry;
import com.yeahmobi.datasystem.query.meta.ReportResult;
import com.yeahmobi.datasystem.query.skeleton.Formatters;
import com.yeahmobi.datasystem.query.utils.Utils;

public class QueryExecuteThread implements Runnable {
    private static Logger logger = Logger.getLogger(QueryExecuteThread.class);

    private QueryEntry query;
    private int delay;

    String query_url = "http://localhost:8080/datasystem-realquery/report?report_param={\"data\":[\"clicks\"],\"filters\":{\"$and\":{\"campaign_id\":{\"$eq\":201405}}},\"group\":[\"hour\",\"time_stamp\",\"aff_id\"],\"settings\":{\"report_id\":\"report_id-100\",\"time\":{\"start\":1399942800,\"end\":1399946399,\"timezone\":0},\"data_source\":\"contrack_druid_datasource_ds\",\"pagination\":{\"size\":10000,\"page\":0}}}";
    private static String callBackUrl = "http://api.yeahmobi.com/Report/setReferraFileReady?unique_key=%s&file_url=%s&verification_code=%s";

    static {
        PropertiesConfiguration cfg = ConfigLoader.getInstance().getConfigurations();
        if (null != cfg) {
            callBackUrl = ConfigKeyConst.parseStr(cfg, callBackUrl, ConfigKeyConst.YEAHMOBI_SERVICE_CALLBACK, null, logger);
        }
    }

    public QueryExecuteThread() {
    }

    public QueryExecuteThread(QueryEntry query, int delay) {
        super();
        this.query = query;
        this.delay = delay;
    }

    public void run() {
        /*
         * // * 调用 解析查询语句 & druid请求发起程序 //
         */
        // try {
        // if (query != null) {
        // if (logger.isDebugEnabled()) {
        // logger.debug("Current activeCount of Sys Thread."+Thread.activeCount());
        // logger.debug("Query [" + new Gson().toJson(query) +
        // "] is up to be Executed...");
        // }
        //
        // String fileStorePath = "";
        // String queryStr = query.getQuery_str();
        // String report_id = query.getKey();
        // if (logger.isDebugEnabled()) {
        // logger.debug("report_id [" + report_id + "].");
        // logger.debug("query [" + queryStr + "].");
        // }
        //
        // InputStream inputStream = null;
        // NetService netService = NetService.getNetService();
        // try {
        // inputStream = netService.fireQuery(queryStr);
        // } catch (ReportParserException e) {
        // logger.error("", e);
        // }
        //
        // ReportContext reportContext = netService.getReportContext();
        // ReportResult reportResult = ProcessService.postProcess(inputStream,
        // reportContext);
        //
        // String tmp_result = new Gson().toJson(reportResult);
        // String csvdatas =
        // Utils.formatResultCSVStr(Utils.getResultNodeXxxContents(tmp_result,
        // Utils.DATA_SEG), true);
        //
        // // fileStorePath = AWSFileStore.Save(report_id, csvdatas);
        // fileStorePath = AWSFileStore.uploadDataAsCSV(report_id,
        // csvdatas).toString();
        //
        // /*
        // * md5（unique_key+file_url+“Yeahmobif3899843bc09ff972ab6252ab3c3cac6”）
        // */
        // // String verificationCode = YeahmobiUtils.MD5(report_id +
        // fileStorePath + "Yeahmobif3899843bc09ff972ab6252ab3c3cac6");
        // String verificationCode = DigestUtils.md5Hex(report_id +
        // fileStorePath + "Yeahmobif3899843bc09ff972ab6252ab3c3cac6");
        //
        // /*
        // *
        // http://api.yeahmobi.com/Report/setReferraFileReady?unique_key=%s&file_url=%s&verification_code=%s
        // */
        // // String bsYeahmobiUrl = String.format(callBackUrl, report_id,
        // fileStorePath, verificationCode);
        // String bsYeahmobiUrl = String.format(callBackUrl, report_id,
        // URLEncoder.encode(fileStorePath, "UTF-8"), verificationCode);
        //
        //
        // if (logger.isDebugEnabled()) {
        // NetService.yeahMobiSSLCallBack(bsYeahmobiUrl);
        // logger.debug("tmp_result [" + tmp_result + "].");
        // logger.debug("csvdatas [" + csvdatas + "].");
        // logger.debug("csv File s3 Storage Path [" + fileStorePath + "].");
        // logger.debug("Bs Yeahmobi Callback Url [" + bsYeahmobiUrl + "].");
        // }else {
        // NetService.yeahMobiCallBack(bsYeahmobiUrl);
        // }
        // /*
        // * 调用druid broker 节点查询操作；
        // * 接收查询结果；
        // * 格式化成csv格式动态字串；
        // * 数据压缩，转储到S3，转储结束后返回下载串；
        // * 封装回调接口，执行回调请求。
        // * over。
        // */
        //
        // }
        // Thread.sleep(delay);
        // } catch (InterruptedException e) {
        // logger.error("", e);
        // } catch (IOException e) {
        // logger.error("An Exception occured while tring to upload to s3 storage.",
        // e);
        // }

        if (null != query) {
        	if(query.isImpala()){
        		
        		ImpalaCallable impalaCallable = new ImpalaCallable(query.getDataSource(), query.getParser());
        		ReportResult ret = null;
				try {
					// 查询impala
					ret = impalaCallable.call();
				} catch (Exception e) {
					logger.error("failed to query impala", e);
				}
 
				// call back 地址
				String callBackUrl = "http://api.yeahmobi.com/Report/setReferraFileReady?unique_key=%s&file_url=%s&verification_code=%s";
				PropertiesConfiguration cfg = ConfigLoader.getInstance().getConfigurations();
				callBackUrl = ConfigKeyConst.parseStr(cfg, callBackUrl, ConfigKeyConst.YEAHMOBI_SERVICE_CALLBACK, null, logger);

				File file = null;
				try {
					file = File.createTempFile(query.getPostContext().getReportParam().getSettings().getReport_id() + "-", "." + query.getFormat());
				} catch (IOException e) {
					logger.error("failed to create tmp file", e);
				}

				// 写入文件， 数据格式
        		try(OutputStream out = new FileOutputStream(file); OutputStream buffOut = new BufferedOutputStream(out)){
        			Formatters.getFormatters().get(query.getFormat()).write(ret, new BufferedOutputStream(buffOut));
        		}catch (Exception e) {
        			logger.error("failed to generate impala result", e);
				}
        		
    	        try {
    	            String report_id = query.getPostContext().getReportParam().getSettings().getReport_id();
    	            String fileStorePath = AWSFileStore.uploadDataAsCSV(file).toString();
    	            FileUtils.deleteQuietly(file);
    	            String verificationCode = DigestUtils.md5Hex(report_id + fileStorePath + "Yeahmobif3899843bc09ff972ab6252ab3c3cac6");
    	            if (logger.isDebugEnabled()) {
    	                logger.debug("file_url=" + fileStorePath);
    	            }
    	            String bsYeahmobiUrl = String.format(callBackUrl, report_id, URLEncoder.encode(fileStorePath, "UTF-8"), verificationCode);
    	            NetService.yeahMobiCallBack(bsYeahmobiUrl);
    	        } catch (IOException e) {
    	            logger.error("", e);
    	        }
        	}else{
        		
        		AsyncHttpClientConfig.Builder builder = new AsyncHttpClientConfig.Builder();
        		builder.setCompressionEnabled(true).setAllowPoolingConnection(true)
        		.setRequestTimeoutInMs((int) TimeUnit.MINUTES.toMillis(20))
        		.setIdleConnectionTimeoutInMs((int) TimeUnit.MINUTES.toMillis(20));
        		AsyncHttpClient client = new AsyncHttpClient(builder.build());
        		
        		AsyncHandler<Boolean> akkaHandler = query.getPostProcess().createFileAkkaHandler();
        		String url = query.getPostProcess().getBrokerUrl();
        		
        		try {
        			client.preparePost(url).addHeader("content-type", "application/json").setBody(query.getQuery_str().getBytes("UTF-8")).execute(akkaHandler)
        			.get();
        		} catch (IllegalArgumentException | InterruptedException | ExecutionException | IOException e) {
        			String msg = e.getMessage();
        			callBackError(msg);
        			logger.error("", e);
        		}
        	}
        }

    }

    public static String csvStringBuilder(InputStream inputStream) {
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
        StringBuilder strBuilder = new StringBuilder();
        String line = null;
        try {
            while (null != (line = bufferedReader.readLine())) {
                strBuilder.append(line).append("\r\n");
            }
        } catch (IOException e) {
            logger.error("试图从查询返回的 数据流中读取数据行时出现异常.", e);
        }
        return strBuilder.toString();
    }

    private String csvHeaderBuilder(String query) {
        StringBuilder stringBuilder = new StringBuilder();

        if (!Strings.isNullOrEmpty(query)) {
            if (Utils.containsGroupNode(query) && Utils.containsDataNode(query)) {
                stringBuilder.append(Utils.getNodeXxxContents(query, Utils.DIM_SEG)).append(Utils.PUNCTUATION_COMMA)
                        .append(Utils.getNodeXxxContents(query, Utils.DATA_SEG)); // "hour","time_stamp","aff_id"
            } else if (Utils.containsGroupNode(query)) {
                stringBuilder.append(Utils.getNodeXxxContents(query, Utils.DIM_SEG));
            } else if (Utils.containsDataNode(query)) {
                stringBuilder.append(Utils.getNodeXxxContents(query, Utils.DATA_SEG));
            }
        }

        return stringBuilder.toString();
    }
    
    
    private void callBackError(String msg){
    	msg = URLEncoder.encode(msg);
    	String report_id = query.getPostContext().getReportParam().getSettings().getReport_id();
        String verificationCode = DigestUtils.md5Hex(report_id + "null" + "Yeahmobif3899843bc09ff972ab6252ab3c3cac6");
    	String bsYeahmobiUrl = String.format(callBackUrl, report_id, null, verificationCode) + "&failMsg=" + msg;
        NetService.yeahMobiCallBack(bsYeahmobiUrl);
    }
}
