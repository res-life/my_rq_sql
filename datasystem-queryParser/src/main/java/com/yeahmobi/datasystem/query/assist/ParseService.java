package com.yeahmobi.datasystem.query.assist;

import io.druid.jackson.DefaultObjectMapper;

import org.apache.log4j.Logger;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.base.Strings;
import com.yeahmobi.datasystem.query.antlr4.DruidReportParser;
import com.yeahmobi.datasystem.query.antlr4.WarpInterpreter;
import com.yeahmobi.datasystem.query.config.ConfigManager;
import com.yeahmobi.datasystem.query.exception.FileErrorListener;
import com.yeahmobi.datasystem.query.exception.ReportParserException;
import com.yeahmobi.datasystem.query.process.QueryContext;
import com.yeahmobi.datasystem.query.process.QueryFactory;
import com.yeahmobi.datasystem.query.utils.YeahmobiUtils;

/**
 * <p>druid查询解析辅助类：获取查询解析器对象，获取查询上下文环境对象，获取查询信息包装类对象</p>
 * @since V1.0
 * @Author Martin
 * @createTime 2014年5月30日 下午1:01:20
 * @modifiedBy name
 * @modifyOn dateTime
 */
public class ParseService {
    private static Logger logger = Logger.getLogger(ParseService.class);
    private DruidReportParser parser = null;
    private QueryContext ctx = null;
    private String queryData = "";
    
    private ParseService() {
        super();
    }

    public static ParseService getParseService() {
        return new ParseService();
    }
    
    private DruidReportParser getStatementParser(String queryString) throws ReportParserException {
        String dataSource = YeahmobiUtils.getDataSourceFromQuery(queryString);
        parser = WarpInterpreter.convert(queryString, dataSource, FileErrorListener.INSTANCE);
        setQueryInfo();
        if (logger.isDebugEnabled()) {
            logger.debug("dataSource [" + dataSource + "].");
            logger.debug("parser [" + parser.orderBy + "].");
        }
        return parser;
    }

    // TODO refactor
//    private QueryInfo validQueryAddr(QueryInfo queryInfo) {
//        if (Strings.isNullOrEmpty(queryInfo.getAddr())) {
//            String addr = ConfigManager.getInstance().getCfg().druid.getSocket();
//            queryInfo.setAddr(addr);
//        }
//        return queryInfo;
//    }
    
    private void setQueryInfo() {
        // replace data source here
//        String[] dims = new String[parser.groupByDimensions.size()];
//        parser.groupByDimensions.keySet().toArray(dims);
//        QueryInfo queryInfo =  new QueryInfo(parser.getDataSource(), dims);
//        this.queryInfo = validQueryAddr(QueryInfoBuilder.getInstance().get(queryInfo));
    	
    	// TODO: refactor
        parser.setDataSource("contrack_druid_datasource_ds");
    }
    
    private QueryContext getQueryContext(String queryString) throws ReportParserException {
        ctx = QueryFactory.create(getStatementParser(queryString), this.queryData);
        return ctx;
    }
    
    public String getJsonQueryStr(String queryString) throws ReportParserException {
        this.queryData = queryString;
        String queryStr = "";
        ObjectMapper objectMapper = new DefaultObjectMapper();
        ObjectWriter jsonWriter = objectMapper.writerWithDefaultPrettyPrinter();
        try {
            queryStr = jsonWriter.writeValueAsString(getQueryContext(ProcessService.preProcess(queryString)).getQuery());
        } catch (JsonProcessingException e) {
            logger.error("Error Occured While Trying to Convert DuidQueryContext-obj to json-obj.", e);
        }
        if (logger.isDebugEnabled()) {
            logger.debug("queryStr [" + queryStr + "].");
        }
        return queryStr;
    }
    
    public DruidReportParser getDruidReportParser() {
        return parser;
    }
    
    public QueryContext getQueryContext() {
        return ctx;
    }

    
//    public static void main(String[] args) {
//        String queryString = "{\"settings\":{\"report_id\":\"report_id-m201405281021\",\"return_format\":\"file\",\"time\":{\"start\":1398902520,\"end\":1398909840,\"timezone\":0},\"data_source\":\"ymds_druid_datasource\",\"pagination\":{\"size\":50,\"page\":0}},\"filters\":{\"$and\":{\"log_tye\":{\"$eq\":1},\"platform_id\":{\"$js\":\"function(x){return(x>=2&&x<=3)}\"}}},\"data\":[(longsum(conversion) as conversion)],\"group\":[\"click_ip\",\"conv_ip\",\"platform_id\",\"click_ip\",\"offer_id\",\"aff_id\"]}";
//        String queryString2 = "{\"data\":[(longsum(click) as click)],\"settings\":{\"report_id\":\"report_id-m201405281021\",\"return_format\":\"file\",\"time\":{\"start\":1398902520,\"end\":1398909840,\"timezone\":0},\"data_source\":\"ymds_druid_datasource\",\"pagination\":{\"size\":50,\"page\":0}},\"filters\":{\"$and\":{\"log_tye\":{\"$eq\":1},\"platform_id\":{\"$js\":\"function(x){return(x>=2&&x<=3)}\"}}},\"group\":[\"click_ip\",\"conv_ip\",\"platform_id\",\"click_ip\",\"offer_id\",\"aff_id\",\"transaction_id\"]}";
//        String queryString3 = "{\"settings\":{\"report_id\":\"report_id-m201405281021\",\"return_format\":\"file\",\"time\":{\"start\":1398902520,\"end\":1398909840,\"timezone\":0},\"data_source\":\"ymds_druid_datasource\",\"pagination\":{\"size\":50,\"page\":0}},\"filters\":{\"$and\":{\"log_tye\":{\"$eq\":1},\"platform_id\":{\"$js\":\"function(x){return(x>=2&&x<=3)}\"}}},\"data\":[(longsum(click) as click)],\"group\":[\"click_ip\",\"conv_ip\",\"platform_id\",\"click_ip\",\"offer_id\",\"aff_id\",\"transaction_id\"]}";
//        String queryString4 = "{\"data\":[(longsum(click) as click)],\"settings\":{\"report_id\":\"report_id-m201405281021\",\"return_format\":\"file\",\"time\":{\"start\":1398902520,\"end\":1398909840,\"timezone\":0},\"data_source\":\"ymds_druid_datasource\",\"pagination\":{\"size\":50,\"page\":0}},\"filters\":{\"$and\":{\"log_tye\":{\"$eq\":1},\"platform_id\":{\"$js\":\"function(x){return(x>=2&&x<=3)}\"}}},\"group\":[\"click_ip\",\"conv_ip\",\"platform_id\",\"click_ip\",\"offer_id\",\"aff_id\",\"transaction_id\"]}";
//        DruidReportParser parser = null;
//        try {
//            parser = WarpInterpreter.convert(queryString4, FileErrorListener.INSTANCE);
//        } catch (ReportParserException e) {
//            e.printStackTrace();
//        }
//        if (parser != null) {
//            System.out.println("DataSource = " + parser.getDataSource());
//        } else {
//            System.out.println("parser == null");
//        }
//    }
}
