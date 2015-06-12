package com.yeahmobi.datasystem.query.extensions;


import com.yeahmobi.datasystem.query.pretreatment.PretreatmentHandler;
import com.yeahmobi.datasystem.query.pretreatment.ReportContext;

import com.yeahmobi.datasystem.query.skeleton.DataSetHandler;
import com.yeahmobi.datasystem.query.skeleton.PostContext;

/**
 * defines all the interfaces of data source plug-in<br>
 * if add one data source, should implements all the interfaces<br>
 *
 */
public interface DataSourceView {
	
	/**
	 * dimentions
	 * @return
	 */
	Dimentions dimentions();
	
	/**
	 * metrics
	 * @return
	 */
	Metrics metrics();
	
	/**
	 * intervalTables
	 * @return
	 */
	IntervalTables intervalTables();
	
	/**
	 * data source specific handler<br>
	 * such as trading desk Landing page feature<br>
	 * data source specific feature should put here<br>
	 * this handler is used to process the druid result<br>
	 * 不能返回null, 如果不需要处理， 返回一个do nothing的handler
	 * @return
	 */
	DataSetHandler getExtraHandler(PostContext reportFeatures);
	
	/**
	 * 获取外部预处理handler
	 * @param reportContext
	 * @return
	 */
	PretreatmentHandler getPreExtraHandler(ReportContext reportContext);
}
