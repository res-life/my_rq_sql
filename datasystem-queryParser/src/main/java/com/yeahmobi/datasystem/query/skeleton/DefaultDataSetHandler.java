package com.yeahmobi.datasystem.query.skeleton;

/**
 * used to handle the data set<br>
 * data set represents the druid result<br>
 * this uses Chain of Responsibility pattern
 * 
 */
public class DefaultDataSetHandler extends DataSetHandler {

	/**
	 * handle the data set
	 * 
	 * @param dataSet
	 * @return
	 */
	public DataSet processDataSet(DataSet dataSet) {

		// 如果有后续的handler进行处理
		DataSet newDataSet = dataSet;

		if (getSuccessor() != null) {
			newDataSet = getSuccessor().processDataSet(newDataSet);
		}

		return newDataSet;
	}
}
