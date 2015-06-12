package com.yeahmobi.datasystem.query.extensions;

import ro.fortsoft.pf4j.ExtensionPoint;


/**
 * 
 * The data source plug-in should extends this extension point<br>
 * If the data source plug-in is istalled<br>
 * then the plug-in will be loaded<br>
 *
 */
public interface InstalledDataSource extends ExtensionPoint{
	
	/**
	 * each data source should implements the plug-in 
	 * @return
	 */
	DataSourcePlugin getPlugin();
}
