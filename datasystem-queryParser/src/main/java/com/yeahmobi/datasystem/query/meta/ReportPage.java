package com.yeahmobi.datasystem.query.meta;
/**
 * Created by yangxu on 3/17/14.
 */

import org.apache.log4j.Logger;

public class ReportPage {

    private static Logger logger = Logger.getLogger(ReportPage.class);
    	public ReportPage() {
	}

	public ReportPage(int number, long total) {
		this.pagenumber = number;
		this.total = total;
	}

	private int pagenumber;
	private long total;

	public int getPagenumber() {
		return pagenumber;
	}

	public void setPagenumber(int number) {
		this.pagenumber = number;
	}

	public long getTotal() {
		return total;
	}

	public void setTotal(int total) {
		this.total = total;
	}

}
