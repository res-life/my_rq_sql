package com.yeahmobi.datasystem.query.akka.http;

/**
 * Created by yangxu on 5/5/14.
 */

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.log4j.Logger;

import akka.actor.ActorRef;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.joda.JodaModule;
import com.google.common.primitives.Bytes;
import com.metamx.common.Pair;
import com.yeahmobi.datasystem.query.akka.cache.CacheTool;
import com.yeahmobi.datasystem.query.antlr4.DruidReportParser;
import com.yeahmobi.datasystem.query.jersey.ReportServiceResult;
import com.yeahmobi.datasystem.query.meta.MsgType;
import com.yeahmobi.datasystem.query.meta.ReportResult;
import com.yeahmobi.datasystem.query.process.QueryContext;
import com.yeahmobi.datasystem.query.reportrequest.DataRange;
import com.yeahmobi.datasystem.query.skeleton.DataSet;
import com.yeahmobi.datasystem.query.skeleton.DataSetHandler;
import com.yeahmobi.datasystem.query.skeleton.InMemoryDataSet;
import com.yeahmobi.datasystem.query.skeleton.PostContext;
import com.yeahmobi.datasystem.query.skeleton.ReportResultDataSet;
import com.yeahmobi.datasystem.query.skeleton.ReportServiceResultDataSet;

public class DruidBodyAsyncConsumer implements BodyConsumer {
	private static Logger logger = Logger.getLogger(DruidBodyAsyncConsumer.class);

	List<Byte> stream = new LinkedList<>();

	ByteStreamJsonParser rowparser;
	JsonFactory jsonFactory = new JsonFactory();
	ObjectMapper mapper = new ObjectMapper().registerModule(new JodaModule());
	boolean sent = false;

	private PostContext reportFeatures;

	DruidReportParser parser;
	QueryContext ctx;
	CacheTool cacheToolL1;
	private CacheTool cacheToolL2;
	ActorRef sender;
	ActorRef receiver;

	private DataSet dataSet;
	private DataSetHandler dataSetHandler;

	public DruidBodyAsyncConsumer(PostContext reportFeatures, DataSet dataSet, CacheTool cacheTool,
			CacheTool cacheToolL2, ActorRef sender, ActorRef receiver, DataSetHandler dataSetHandler) {
		this.reportFeatures = reportFeatures;
		this.dataSet = dataSet;
		this.parser = reportFeatures.getParser();
		this.ctx = reportFeatures.getQueryContext();
		this.cacheToolL1 = cacheTool;
		this.cacheToolL2 = cacheToolL2;
		this.sender = sender;
		this.receiver = receiver;
		this.dataSetHandler = dataSetHandler;
		this.rowparser = new ByteStreamJsonParser(jsonFactory, mapper, ctx.getElemType());
	}

	DruidBodyAsyncConsumer(DataSet dataSet){
		this.dataSet = dataSet;
	}
	
	public void write(byte[] bytes) throws IOException {
		if(null == bytes){
			return;
		}
		stream.addAll(Bytes.asList(bytes));
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
		if (!sent) {
			DataSet subDataSet = null;
			int start = DataRange.getStart(parser.page, parser.size, parser.offset);
			int end = DataRange.getEnd(parser.page, parser.size, parser.offset);

			if (end <= dataSet.size() || sendAll) { // page starts from zero
				if (start > dataSet.size() - 1) {
					if (dataSet instanceof InMemoryDataSet)
						subDataSet = new InMemoryDataSet(null);
				} else {
					subDataSet = dataSet.subDataSet(start, end > dataSet.size() ? dataSet.size() : end);
				}
			}
			if (null != subDataSet) {
				// 先发送这一页数据
				DataSet retDataSet = dataSetHandler.processDataSet(subDataSet);
				ReportServiceResult reportServiceResult = null;

				if (retDataSet instanceof ReportServiceResultDataSet) {
					reportServiceResult = retDataSet.getReportServiceResult();
				} else if (retDataSet instanceof ReportResultDataSet) {
					reportServiceResult = new ReportServiceResult(MsgType.success, retDataSet.getReportResult());
				} else {
					throw new RuntimeException("can't handle this data set");
				}

				// 缓存当前页到一级缓存
				if (cacheToolL1 != null) {
					cacheToolL1.set(reportFeatures.getReportParam(), retDataSet.getReportResult());
					logger.info("saved result to Level 1 cache");
				}

				sender.tell(reportServiceResult, receiver);
				sent = true;
			}
		}

		return sent;
	}

	public boolean tryCache() {
		if (null == cacheToolL2) {
			logger.info("L2 cache is null, will not save to L2 cache");
			return false;
		}
		
		DataSet retDataSet = dataSetHandler.processDataSet(dataSet);
		ReportResult ret = retDataSet.getReportResult();

		// 所有页缓存到二级缓存
		int limit = parser.maxRows;
		int rowSize = dataSet.size();
		// 查询到的结果数小于limit, 说明是全量数据， 可以使用sort
		boolean isFullData = rowSize < limit;
		
		cacheToolL2.set(reportFeatures.getReportParam(), ret, isFullData);

		return true;
	}

	public boolean tryClose() throws IOException {
		stream.clear();
		return true;
	}

	public boolean hasSent() {
		return sent;
	}
}

