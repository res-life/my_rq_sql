package com.yeahmobi.datasystem.query.pagination;
/**
 * Created by yangxu on 3/25/14.
 */

public class PageMeta {

    int pageSize;
    int pageNum;

    public PageMeta() {
    }

    public PageMeta(int pageSize, int pageNum) {
        this.pageSize = pageSize;
        this.pageNum = pageNum;
    }

    public int getPageSize() {
        return pageSize;
    }

    public void setPageSize(int pageSize) {
        this.pageSize = pageSize;
    }

    public int getPageNum() {
        return pageNum;
    }

    public void setPageNum(int pageNum) {
        this.pageNum = pageNum;
    }
}
