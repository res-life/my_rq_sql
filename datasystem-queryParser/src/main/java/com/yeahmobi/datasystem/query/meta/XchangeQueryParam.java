package com.yeahmobi.datasystem.query.meta;

public class XchangeQueryParam {
    private XchangeQueryType query_type;
    private XchangeReturnFormat return_format;
    private String query_id;
    private String[] colums;
    
    public XchangeQueryType getQuery_type() {
        return query_type;
    }
    public void setQuery_type(XchangeQueryType query_type) {
        this.query_type = query_type;
    }
    public XchangeReturnFormat getReturn_format() {
        return return_format;
    }
    public void setReturn_format(XchangeReturnFormat return_format) {
        this.return_format = return_format;
    }
    public String getQuery_id() {
        return query_id;
    }
    public void setQuery_id(String query_id) {
        this.query_id = query_id;
    }
    public String[] getColums() {
        return colums;
    }
    public void setColums(String[] colums) {
        this.colums = colums;
    }
}
