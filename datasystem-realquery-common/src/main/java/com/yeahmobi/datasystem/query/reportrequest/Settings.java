package com.yeahmobi.datasystem.query.reportrequest;
/**
 * setting节点
 * @author dylan.zhang@yeahmobi.com
 * @date 2014年9月11日 下午5:23:24
 */
public class Settings {
	private String report_id;
	private String data_source;
	private Time time;
	private Pagination pagination=new Pagination(50,0);
	private String return_format;
	private String process_type;
	
	public static class Time{
		private long start;
		private long end;
		private double timezone=0;
		public Time() {}
		public Time(long start, long end, double timezone) {
			this.start = start;
			this.end = end;
			this.timezone = timezone;
		}
		public long getStart() {
			return start;
		}
		public void setStart(long start) {
			this.start = start;
		}
		public long getEnd() {
			return end;
		}
		public void setEnd(long end) {
			this.end = end;
		}
		public double getTimezone() {
			return timezone;
		}
		public void setTimezone(double timezone) {
			this.timezone = timezone;
		}
		
	}
	
	public static class Pagination{
		private int size;
		private int page;
		private int offset = -1;
		
		public Pagination(){};
		public Pagination(int size, int page) {
			this.size = size;
			this.page = page;
		}
		public Pagination(int size, int page, int offset) {
			this.size = size;
			this.page = page;
			this.offset = offset;
		}
		public int getSize() {
			return size;
		}
		public void setSize(int size) {
			this.size = size;
		}
		public int getPage() {
			return page;
		}
		public void setPage(int page) {
			this.page = page;
		}
		public int getOffset() {
			return offset;
		}
		public void setOffset(int offset) {
			this.offset = offset;
		}
	}

	public String getReport_id() {
		return report_id;
	}

	public void setReport_id(String report_id) {
		this.report_id = report_id;
	}

	public String getData_source() {
		return data_source;
	}

	public void setData_source(String data_source) {
		this.data_source = data_source;
	}

	public Time getTime() {
		return time;
	}

	public void setTime(Time time) {
		this.time = time;
	}


	public Pagination getPagination() {
		return pagination;
	}

	public void setPagination(Pagination pagination) {
		this.pagination = pagination;
	}


	public String getReturn_format() {
		return return_format;
	}

	public void setReturn_format(String return_format) {
		this.return_format = return_format;
	}

	public String getProcess_type() {
		return process_type;
	}

	public void setProcess_type(String process_type) {
		this.process_type = process_type;
	}
	
	
}
