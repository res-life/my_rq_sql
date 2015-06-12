package com.yeahmobi.datasystem.query.skeleton.handler;

import com.yeahmobi.datasystem.query.skeleton.DataSet;
import com.yeahmobi.datasystem.query.skeleton.DefaultDataSetHandler;

/**
 * 一级缓存, 使用redis或者memcache<br>
 * 一级缓存值只保存第一页数据<br>
 */
public class CachHandler extends DefaultDataSetHandler{

	public DataSet processDataSet(DataSet dataSet) {

		
		DataSet newDataSet = null;

		// 如果有后续的handler进行处理
		return super.processDataSet(newDataSet);
	}
}
