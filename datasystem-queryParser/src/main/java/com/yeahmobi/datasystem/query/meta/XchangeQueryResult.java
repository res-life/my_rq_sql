package com.yeahmobi.datasystem.query.meta;

import java.util.List;


public class XchangeQueryResult{
    private String flag;
    private String msg;
    private List<XCHANGE_RATE> rates;
    
    public XchangeQueryResult() {}

    public XchangeQueryResult(String flag, String msg, List<XCHANGE_RATE> rates) {
        super();
        this.flag = flag;
        this.msg = msg;
        this.rates = rates;
    }

    public String getFlag() {
        return flag;
    }
    public void setFlag(String flag) {
        this.flag = flag;
    }

    public String getMsg() {
        return msg;
    }
    public void setMsg(String msg) {
        this.msg = msg;
    }

    public List<XCHANGE_RATE> getRates() {
        return rates;
    }
    public void setRates(List<XCHANGE_RATE> rates) {
        this.rates = rates;
    }
}
