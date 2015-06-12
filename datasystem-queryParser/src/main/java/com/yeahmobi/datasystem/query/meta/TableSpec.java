package com.yeahmobi.datasystem.query.meta;

/**
 * Created by yangxu on 5/12/14.
 */
public interface TableSpec {

    public String getDataSource();

    public void init();

    public void init(String ds);

    public void copy(TableSpec spec);
}
