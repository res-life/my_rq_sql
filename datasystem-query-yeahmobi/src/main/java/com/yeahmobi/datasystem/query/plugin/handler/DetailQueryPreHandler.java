package com.yeahmobi.datasystem.query.plugin.handler;

import com.google.common.collect.ImmutableList;
import com.yeahmobi.datasystem.query.pretreatment.PretreatmentHandler;
import com.yeahmobi.datasystem.query.pretreatment.ReportContext;

/**
 * 
 * @author joker
 * 
 */
public class DetailQueryPreHandler extends PretreatmentHandler {
    private static final String CLICK_STRING = "click";
    private static final String TRANSACTION_ID = "transaction_id";

    @Override
    public void handleRequest(ReportContext reportContext) {
        if (!reportContext.isSyncProcess()&&reportContext.isDetailQuery()) {

            reportContext = setDataNode(reportContext);
            reportContext = setDimNode(reportContext);
        }
        if (getNextHandler() != null) {
            getNextHandler().handleRequest(reportContext);
        }
    }

    // set data node if the dataNode is null,then add the 'click' as default
    // value
    public ReportContext setDataNode(ReportContext reportContext) {

        if (null == reportContext.getReportParam().getData()) {
            reportContext.getReportParam().setData(ImmutableList.of(CLICK_STRING));
        } else if(reportContext.getReportParam().getGroup().size() == 0 ) {
            reportContext.getReportParam().getData().add(CLICK_STRING);
        }

        return reportContext;
    }

    // add the 'transaction_id' into the group List,because of this is detail
    // query
    public ReportContext setDimNode(ReportContext reportContext) {

        if (null == reportContext.getReportParam().getGroup()) {
            reportContext.getReportParam().setGroup(ImmutableList.of(TRANSACTION_ID));
        } else if(reportContext.getReportParam().getGroup().size() == 0 ) {
            reportContext.getReportParam().getGroup().add(TRANSACTION_ID);
        }

        return reportContext;
    }

}
