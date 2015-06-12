package com.yeahmobi.datasystem.query.skeleton;

import java.util.List;
import java.util.Map;

import com.yeahmobi.datasystem.query.jersey.ReportServiceResult;
import com.yeahmobi.datasystem.query.meta.ReportResult;

/**
 * Data set 的默认实现<br>
 * 每个方法都是throw 异常
 * 
 *
 */
public class AbstractDataSet implements DataSet {

	@Override
	public PostContext getRequest() {
		throw new UnsupportedOperationException();
	}

	@Override
	public void addRow(Object row) {
		throw new UnsupportedOperationException();
	}

	@Override
	public List<String> getHeader() {
		throw new UnsupportedOperationException();
	}

	@Override
	public void flush() {
		throw new UnsupportedOperationException();
	}

	@Override
	public List<Object> getAllData() {
		throw new UnsupportedOperationException();
	}

	@Override
	public int size() {
		throw new UnsupportedOperationException();
	}

	@Override
	public List<Object> subList(int fromIndex, int toIndex) {
		throw new UnsupportedOperationException();
	}

	@Override
	public List<List<Object>> partition(int size) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Map<String, String> getInfo() {
		throw new UnsupportedOperationException();
	}

	@Override
	public ReportResult getReportResult() {
		throw new UnsupportedOperationException();
	}

	@Override
	public ReportServiceResult getReportServiceResult() {
		throw new UnsupportedOperationException();
	}

	@Override
	public DataSet subDataSet(int fromIndex, int toIndex) {
		throw new UnsupportedOperationException();
	}

	@Override
	public List<DataSet> subDataSets(int size) {
		// TODO Auto-generated method stub
		return null;
	}
}
