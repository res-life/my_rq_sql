package com.yeahmobi.datasystem.query.meta;

import com.yeahmobi.datasystem.query.antlr4.DruidReportParser;
import com.yeahmobi.datasystem.query.process.QueryContext;

/**
 * @author martin
 *
 */
public class ReportContext {
    private QueryContext queryContext;
    private DruidReportParser druidReportParser;
    
    public ReportContext() {}
    public ReportContext(QueryContext queryContext, DruidReportParser druidReportParser) {
        super();
        this.queryContext = queryContext;
        this.druidReportParser = druidReportParser;
    }
    
    public QueryContext getQueryContext() {
        return queryContext;
    }
    public void setQueryContext(QueryContext queryContext) {
        this.queryContext = queryContext;
    }
    public DruidReportParser getDruidReportParser() {
        return druidReportParser;
    }
    public void setDruidReportParser(DruidReportParser druidReportParser) {
        this.druidReportParser = druidReportParser;
    }
}
