package com.yeahmobi.datasystem.query.akka.http;

/**
 * Created by yangxu on 5/5/14.
 */

import java.io.File;
import java.io.IOException;
import java.net.URLEncoder;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import akka.actor.ActorRef;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.joda.JodaModule;
import com.metamx.common.Pair;
import com.yeahmobi.datasystem.query.akka.cache.CacheTool;
import com.yeahmobi.datasystem.query.antlr4.DruidReportParser;
import com.yeahmobi.datasystem.query.assist.NetService;
import com.yeahmobi.datasystem.query.aws.AWSFileStore;
import com.yeahmobi.datasystem.query.process.QueryContext;
import com.yeahmobi.datasystem.query.queue.DetailConstant;
import com.yeahmobi.datasystem.query.skeleton.DataSet;
import com.yeahmobi.datasystem.query.skeleton.DataSetHandler;
import com.yeahmobi.datasystem.query.skeleton.InMemoryDataSet;
import com.yeahmobi.datasystem.query.skeleton.PostContext;

public class DruidBodyAsyncDetailConsumer implements BodyConsumer {
    private static Logger logger = Logger.getLogger(DruidBodyAsyncDetailConsumer.class);

    List<Byte> stream = new LinkedList<>();

    JsonFactory jsonFactory = new JsonFactory();
    ObjectMapper mapper = new ObjectMapper().registerModule(new JodaModule());
    boolean sent = false;
    private static final int BUFFER_SIZE = 1000;
    private PostContext reportFeatures;

    final DruidReportParser parser;
    final QueryContext ctx;
    final CacheTool cacheTool;
    final ActorRef sender;
    final ActorRef receiver;
    private DataSet retDataSet;
    private DataSet dataSet;
    private DataSetHandler dataSetHandler;

	private ByteStreamJsonParser rowparser;

    public DruidBodyAsyncDetailConsumer(PostContext reportFeatures, DataSet dataSet, CacheTool cacheTool, ActorRef sender, ActorRef receiver,
            DataSetHandler dataSetHandler) {
        this.reportFeatures = reportFeatures;
        this.dataSet = dataSet;
        this.parser = reportFeatures.getParser();
        this.ctx = reportFeatures.getQueryContext();
        this.cacheTool = cacheTool;
        this.sender = sender;
        this.receiver = receiver;
        this.dataSetHandler = dataSetHandler;
        this.rowparser = new ByteStreamJsonParser(jsonFactory, mapper, ctx.getElemType());
    }

    public void write(byte[] bytes) throws IOException {
		if(null == bytes){
			return;
		}
		for(Byte b : bytes){
			stream.add(b);
		}
    }

    public int tryParse() throws IOException {
		List<Object> ret = rowparser.tryParse(stream);
		stream.clear();

		for(Object row : ret){
			dataSet.addRow(row);
		}
		
		return ret.size();
    }

    public boolean trySend(boolean sendAll) {

        DataSet subDataset = null;

        if (dataSet.size() >= BUFFER_SIZE) {
            subDataset = dataSet.subDataSet(0, dataSet.size());

        }
        if (null != subDataset) {

            retDataSet = dataSetHandler.processDataSet(subDataset);
            dataSet = new InMemoryDataSet(reportFeatures);
        }

        return sent;
    }

    public boolean tryCache() {
        return true;
    }

    public boolean tryClose() throws IOException {
        DataSet subDataset = dataSet.subDataSet(0, dataSet.size());
        if (null != subDataset) {

            retDataSet = dataSetHandler.processDataSet(subDataset);
            dataSet = new InMemoryDataSet(reportFeatures);
        }
        stream.clear();
        Map<String, String> map = retDataSet.getInfo();
        if (map.containsKey(DetailConstant.FILE_URL) && map.containsKey(DetailConstant.CALLBACK_URL)) {

            uploadFile(map.get(DetailConstant.FILE_URL), map.get(DetailConstant.CALLBACK_URL));
        }
        return true;
    }

    public void uploadFile(String filePath, String callbackUrl) {
        try {
            File file = new File(filePath);

            String report_id = reportFeatures.getReportParam().getSettings().getReport_id();
            String fileStorePath = AWSFileStore.uploadDataAsCSV(file).toString();
            FileUtils.deleteQuietly(file);
            /*
             * md5（unique_key+file_url+“Yeahmobif3899843bc09ff972ab6252ab3c3cac6”
             * ）
             */
            // String verificationCode = YeahmobiUtils.MD5(report_id +
            // fileStorePath
            // + "Yeahmobif3899843bc09ff972ab6252ab3c3cac6");
            String verificationCode = DigestUtils.md5Hex(report_id + fileStorePath + "Yeahmobif3899843bc09ff972ab6252ab3c3cac6");

            /*
             * http://api.yeahmobi.com/Report/setReferraFileReady?unique_key=%s&
             * file_url =%s&verification_code=%s
             */
            // String bsYeahmobiUrl = String.format(callBackUrl, report_id,
            // fileStorePath, verificationCode);
            if (logger.isDebugEnabled()) {
                logger.debug("file_url=" + fileStorePath);
            }
            String bsYeahmobiUrl = String.format(callbackUrl, report_id, URLEncoder.encode(fileStorePath, "UTF-8"), verificationCode);
            NetService.yeahMobiCallBack(bsYeahmobiUrl);
        } catch (IOException e) {
            logger.error("", e);
        }
    }

    public boolean hasSent() {
        return sent;
    }
}
