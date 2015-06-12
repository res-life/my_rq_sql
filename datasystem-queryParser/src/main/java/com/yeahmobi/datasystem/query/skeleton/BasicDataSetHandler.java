package com.yeahmobi.datasystem.query.skeleton;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import com.yeahmobi.datasystem.query.exception.ReportRuntimeException;
import com.yeahmobi.datasystem.query.landingpage.H2InMemoryDbUtil;
import com.yeahmobi.datasystem.query.meta.ReportResult;
import com.yeahmobi.datasystem.query.process.PostProcessor;

/**
 * 精度处理， 默认值处理器,...,原来的逻辑
 * 
 */
public class BasicDataSetHandler extends DefaultDataSetHandler {

	private PostContext request;
	private PostProcessor postProcessor;

	public BasicDataSetHandler(PostContext request, PostProcessor postProcessor) {
		this.request = request;
		this.postProcessor = postProcessor;
	}

	@Override
	public DataSet processDataSet(DataSet dataSet) {

		List<Object> input = dataSet.getAllData();

		Map<String, String> additionalInfo = new HashMap<>();

		if (request.isInsertDb()) {
			
			if (dataSet instanceof DataBaseDataSet) {

				DataBaseDataSet dataBaseDataSet = (DataBaseDataSet) dataSet;
				String firstTableName = dataBaseDataSet.getTableName();
				additionalInfo = dataBaseDataSet.getInfo();
				additionalInfo.put("firstTableName", firstTableName);
					
				if(!request.getReportContext().isDoTimeDb() && !request.isTimeSort()){
					//由于做缓存需要最终表的数据即为最终结果，之前部分是通过后处理来处理的
					alterLastTableDelTim(dataBaseDataSet.getInfo().get("lastTableName"));
				}
				
			} else {
				throw new ReportRuntimeException("can't handle this type " + dataSet.getClass());
			}

		}

		// process the post process
		ReportResult ret = postProcessor.process(input);
		ReportResultDataSet newDataSet = new ReportResultDataSet(ret);
		newDataSet.setInfo(additionalInfo);

		return super.processDataSet(newDataSet);
	}
	
	
	private void alterLastTableDelTim(String tableName){
			
		String sql = "alter table " + tableName + " drop column timestamp";
		String errorMsg = String.format("create update failed, sql is "
				+ sql);
		H2InMemoryDbUtil.executeDbStatement(sql, errorMsg);
	}

	private static final Logger logger = Logger.getLogger(BasicDataSetHandler.class);
}
