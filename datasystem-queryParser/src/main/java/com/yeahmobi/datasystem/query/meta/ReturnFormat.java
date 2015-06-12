package com.yeahmobi.datasystem.query.meta;

/**
 * 返回格式
 * @author chenyi
 * 
 */
public class ReturnFormat {
    private String return_format;//目前取值为file、json

    public ReturnFormat() {
    }

    public ReturnFormat(String return_format) {
        super();
        this.return_format = return_format;
    }

    public String getReturn_format() {
        return return_format;
    }

    public void setReturn_format(String return_format) {
        this.return_format = return_format;
    }
}
