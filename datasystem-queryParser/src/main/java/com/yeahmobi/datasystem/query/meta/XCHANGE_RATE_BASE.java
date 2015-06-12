package com.yeahmobi.datasystem.query.meta;

public class XCHANGE_RATE_BASE {
    public XCHANGE_RATE_BASE(String currency_from, String currency_to, Double rate_from_to, Double rate_usd_to) {
        this.currency_from = currency_from;
        this.currency_to = currency_to;
        this.rate_from_to = rate_from_to;
        this.rate_usd_to = rate_usd_to;
    }
    public XCHANGE_RATE_BASE() {}
    
    public String getCurrency_from() {
        return currency_from;
    }
    public void setCurrency_from(String currency_from) {
        this.currency_from = currency_from;
    }
    public String getCurrency_to() {
        return currency_to;
    }
    public void setCurrency_to(String currency_to) {
        this.currency_to = currency_to;
    }
    public Double getRate_from_to() {
        return rate_from_to;
    }
    public void setRate_from_to(Double rate_from_to) {
        this.rate_from_to = rate_from_to;
    }
    public Double getRate_usd_to() {
        return rate_usd_to;
    }
    public void setRate_usd_to(Double rate_usd_to) {
        this.rate_usd_to = rate_usd_to;
    }
    
    private String currency_from;
    private String currency_to;
    private Double rate_from_to;
    private Double rate_usd_to ;
}
