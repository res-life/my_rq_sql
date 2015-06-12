package com.yeahmobi.datasystem.query.extensions;

/**
 * represents the data source plug-in<br>
 * the data source plug-in can be dynamicly added<br>
 * 
 */
public class DataSourcePlugin {

	/**
	 * data source name
	 */
	private String dataSourceName;
	
	/**
	 * all the data source interfaces is in this view<br>
	 * it represent this data source plug-in
	 */
	private DataSourceView dataSourceView;

	/**
	 * constructor
	 * @param dataSourceName
	 * @param dataSourceView
	 */
	public DataSourcePlugin(String dataSourceName, DataSourceView dataSourceView){
		this.dataSourceName = dataSourceName;
		this.dataSourceView = dataSourceView;
	}
	
	/**
	 * Getter
	 * @return
	 */
	public String getDataSourceName() {
		return dataSourceName;
	}

	/**
	 * Setter
	 * @return
	 */
	public DataSourceView getDataSourceView() {
		return dataSourceView;
	}
}
