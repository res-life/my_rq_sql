package com.yeahmobi.datasystem.query.skeleton;

import java.util.Map;

import com.yeahmobi.datasystem.query.jersey.ReportServiceResult;
import com.yeahmobi.datasystem.query.meta.ReportResult;

/**
 * 包装ReportServiceResult成DataSet
 * 
 */
public class ReportServiceResultDataSet extends AbstractDataSet {

    private ReportServiceResult reportServiceResult;
    private ReportResult reportResult;
    private Map<String, String> info;

    public ReportServiceResultDataSet(ReportServiceResult reportServiceResult, ReportResult reportResult) {
        this.reportServiceResult = reportServiceResult;
        this.reportResult = reportResult;
    }

    public ReportServiceResultDataSet(ReportServiceResult reportServiceResult, Map<String, String> info) {
        this.reportServiceResult = reportServiceResult;
        this.info = info;
    }

    @Override
    public Map<String, String> getInfo() {
        return info;
    }

    @Override
    public ReportServiceResult getReportServiceResult() {
        return reportServiceResult;
    }

    @Override
    public ReportResult getReportResult() {
        return reportResult;
    }

    public void setInfo(Map<String, String> info) {
        this.info = info;
    }

}
