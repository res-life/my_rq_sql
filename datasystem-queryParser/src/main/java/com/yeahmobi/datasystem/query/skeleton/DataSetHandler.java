package com.yeahmobi.datasystem.query.skeleton;

/**
 * used to handle the data set<br>
 * data set represents the druid result<br>
 * this uses Chain of Responsibility pattern
 * 
 */
public abstract class DataSetHandler {

	/**
	 * success handler
	 */
	private DataSetHandler successor = null;

	/**
	 * Getter
	 */
	public DataSetHandler getSuccessor() {
		return successor;
	}

	/**
	 * Setter
	 * 
	 * @param successor
	 */
	public void setSuccessor(DataSetHandler successor) {
		this.successor = successor;
	}

	/**
	 * handle the data set
	 * 
	 * @param dataSet
	 * @return
	 */
	public abstract <T extends DataSet> T processDataSet(T dataSet);

}
