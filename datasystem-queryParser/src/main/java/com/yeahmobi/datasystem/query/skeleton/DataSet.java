package com.yeahmobi.datasystem.query.skeleton;

import java.util.List;
import java.util.Map;

import com.yeahmobi.datasystem.query.jersey.ReportServiceResult;
import com.yeahmobi.datasystem.query.meta.ReportResult;

/**
 * interface that represents the druid result<br>
 * this data set can be saved to memory<br>
 * or can be saved to in-memory database<br>
 * <br>
 * 
 *  当保存在内存中时 Object should be: <br>
 *  	Result<TimeseriesResultValue> for TimeSeries<br>
 *  	MapBasedRow for groupby<br>
 *  	Result<TopNResultValue> for topN<br>
 *  当保存在数据库时 Object 应该是List<Object><br>
 *  	
 */
public interface DataSet {
	
	/**
	 * get the request meta info
	 * @return
	 */
	PostContext getRequest();
	
	/**
	 * add row
	 * @param row
	 */
	void addRow(Object row);
	
	/**
	 * 返回表头
	 * @return
	 */
	List<String> getHeader();
	
	/**
	 * flush the added row to database or other data sink
	 */
	void flush();	
	
	/**
	 * get the data
	 * @param start
	 * @param size
	 * @return
	 */
	List<Object> getAllData();
	
	/**
	 * get the size of data
	 * @return
	 */
	int size();
	
	List<Object> subList(int fromIndex, int toIndex);
	
	DataSet subDataSet(int fromIndex, int toIndex);
	
	List<List<Object>> partition(int size);
	
	List<DataSet> subDataSets(int size);
	
	
	/**
	 * 返回一些额外信息<br>
	 * 比如数据库的DataSet， 返回最终表的表名称
	 * @return
	 */
	Map<String, String> getInfo();
	
	
	ReportResult getReportResult();
	
	ReportServiceResult getReportServiceResult();
	
}
