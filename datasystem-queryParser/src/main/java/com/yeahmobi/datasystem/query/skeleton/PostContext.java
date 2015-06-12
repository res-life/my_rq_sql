package com.yeahmobi.datasystem.query.skeleton;

import java.util.HashMap;
import java.util.Map;

import com.google.common.base.Strings;
import com.yeahmobi.datasystem.query.antlr4.DruidReportParser;
import com.yeahmobi.datasystem.query.pretreatment.ReportContext;
import com.yeahmobi.datasystem.query.process.QueryContext;
import com.yeahmobi.datasystem.query.process.QueryType;
import com.yeahmobi.datasystem.query.reportrequest.ReportParam;

/**
 * 解析 report request<br>
 * 确定请求的特点<br>
 * 是report的context<br>
 */
public class PostContext {

	private ReportContext reportContext;
	
	private QueryContext queryContext;
	
	private DruidReportParser parser;
	
	private Map<String , Map<String,String>> fieldTypeMap = new HashMap<String, Map<String,String>>();

	private String tableName;
	
	public DruidReportParser getParser() {
		return parser;
	}

	public QueryContext getQueryContext() {
		return queryContext;
	}

	public PostContext(ReportContext reportContext, QueryContext queryContext, DruidReportParser parser) {
		this.reportContext = reportContext;
		this.queryContext = queryContext;
		this.parser = parser;
	}

	public ReportParam getReportParam() {
		return reportContext.getReportParam();
	}
	
	public ReportContext getReportContext(){
		return reportContext;
	}
	
	public boolean isInsertDb() {
		String process_type = getReportParam().getSettings().getProcess_type();
		if(Strings.isNullOrEmpty(process_type))
			process_type = "";
		
		if (getReportContext().isDoTimeDb()
				|| process_type.contains("lp")
				|| isOrderBy()) {
			return true;
		} else {
			return false;
		}
	}

	public Map<String, Map<String, String>> getFieldTypeMap() {
		return fieldTypeMap;
	}

	public void setFieldTypeMap(Map<String, Map<String, String>> fieldTypeMap) {
		this.fieldTypeMap = fieldTypeMap;
	}
	
	private boolean isOrderBy(){
		if(parser.timeColumns.size() > 0){
			return true;
		}else if(parser.columns.size() > 0 && getqueryType(parser) == QueryType.TIMESERIES){
			return true;
		}else{
			return false;
		}
	}
	
	public boolean isTimeSort(){
		if(parser.timeColumns.size() > 0 && parser.intervalUnits.size() > 0){
			return true;
		}
		return false;
	}
	
	private QueryType getqueryType(DruidReportParser parser){
		if (parser.groupByDimensions.isEmpty() && parser.threshold == -1) {
			return QueryType.TIMESERIES;
		} else if (parser.threshold != -1){
			return QueryType.TOPN;
		} else{
			return QueryType.GROUPBY;
		}
	}
	
	public boolean forLpTimeFilter(){
		String process_type = getReportParam().getSettings().getProcess_type();
		if(Strings.isNullOrEmpty(process_type))
			process_type = "";
		
		if (!getReportContext().isDoTimeDb()
				&& !isTimeSort()){
			if(parser.intervalUnits.size() > 0 || !parser.timeFilters.isEmpty() || parser.timeColumns.size() > 0)
				return true;
		}
		return false;
	}
	
	public boolean useForParserDb() {
		if (getReportContext().isDoTimeDb()
				|| isOrderBy()) {
			return true;
		} else {
			return false;
		}
	}
	
	public boolean useForParserResult() {
		if (!getReportContext().isDoTimeDb()
				&& parser.columns.size() > 0 && parser.timeColumns.size() == 0 && parser.intervalUnits.size() > 0) {
			return false;
		} else {
			return true;
		}
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public String getTableName() {
		return tableName;
	}
}
