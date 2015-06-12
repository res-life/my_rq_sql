package com.yeahmobi.datasystem.query.jersey;

/**
 * Created by yangxu on 5/5/14.
 */

import javax.ws.rs.Consumes;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.log4j.Logger;
import org.rmrodrigues.pf4j.web.PluginManagerHolder;

import ro.fortsoft.pf4j.PluginManager;

import com.yeahmobi.datasystem.query.skeleton.DataSourceViews;
import com.yeahmobi.datasystem.query.skeleton.Formatters;

/**
 * this service is used to reload plugins,<br>
 * so supports dynamic deploy.<br>
 * if add new data source plug-in or other plug-in, <br>
 * can invoke this to take effective without restart<br>
 * 
 */
@Path("/reload_plugin")
public class PluginService {

	private static Logger logger = Logger.getLogger(PluginService.class);

	/**
	 * reload the plugins
	 * TODO need password to protect?
	 * @return
	 */
	@PUT
	@Consumes(MediaType.TEXT_PLAIN)
	@Produces(MediaType.TEXT_PLAIN)
	public Response reloadPlugin() {

		PluginManager pluginManager = PluginManagerHolder.getPluginManager();
		
		// reload plugins 
		pluginManager.loadPlugins();
		
		// start plugins
		pluginManager.startPlugins();

		// init the data source plug-in
		DataSourceViews.init();
		
		// 重新加载formatter plug ins
		Formatters.init();

		String msg = "success to reload plugins.\r\n";
		logger.info(msg);
		return Response.status(200).entity(msg).build();
	}
}
