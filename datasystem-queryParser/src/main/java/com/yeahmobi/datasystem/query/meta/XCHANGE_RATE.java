package com.yeahmobi.datasystem.query.meta;


public class XCHANGE_RATE extends XCHANGE_RATE_BASE {
    // private String currency_from;
    // private String currency_to;
    // private Double rate_from_to;
    // private Double rate_usd_to ;
    public XCHANGE_RATE() {
        super();
    }
    public XCHANGE_RATE(String currency_from, String currency_to, Double rate_from_to, Double rate_usd_to, Integer id, Integer is_deleted,
            String last_update_time) {
        super(currency_from, currency_to, rate_from_to, rate_usd_to);
        this.id = id;
        this.is_deleted = is_deleted;
        this.last_update_time = last_update_time;
    }
    
    public Integer getId() {
        return id;
    }
    public void setId(Integer id) {
        this.id = id;
    }
    public Integer getIs_deleted() {
        return is_deleted;
    }
    public void setIs_deleted(Integer is_deleted) {
        this.is_deleted = is_deleted;
    }
    public String getLast_update_time() {
        return last_update_time;
    }
    public void setLast_update_time(String last_update_time) {
        this.last_update_time = last_update_time;
    }
    private Integer id ;
    private Integer is_deleted ;
    private String last_update_time;
}
