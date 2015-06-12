package com.yeahmobi.datasystem.query.meta;

import com.google.gson.Gson;

/**
 * Created by yangxu on 5/16/14.
 */

public final class Results {

    public static final String NullParam;
    static {
        ReportResult nullParam = new ReportResult();
        nullParam.setFlag("fail");
        nullParam.setMsg("report_param must not be blank");
        nullParam.setData(null);
        NullParam = new Gson().toJson(nullParam);
    }

    public static final String TimeOut;
    static {
        ReportResult timeout = new ReportResult();
        timeout.setFlag("fail");
        timeout.setMsg("time out");
        timeout.setData(null);
        TimeOut = new Gson().toJson(timeout);
    }

    public static final ReportResult Exception;
    static {
    	Exception = new ReportResult();
    	Exception.setFlag("fail");
    	Exception.setMsg("Exception occured");
    	Exception.setData(null);
    }

}
