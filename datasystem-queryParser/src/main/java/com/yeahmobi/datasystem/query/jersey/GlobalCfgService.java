package com.yeahmobi.datasystem.query.jersey;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.log4j.Logger;

import com.yeahmobi.datasystem.query.meta.GlobalCfg;

@Path("/global_cfg")
public class GlobalCfgService {

	private static Logger logger = Logger.getLogger(GlobalCfgService.class);

	/**
	 * reload the cache cfg <br>
	 * TODO need password to protect? <br>
	 * 
	 * @return
	 */
	@POST
	@Consumes(MediaType.APPLICATION_JSON)
	public Response reloadCacheCfg(GlobalCfg cfg) {
		try {

			String msg = null;
			
			// 重新设置全局变量
			if(cfg.getMaxPageNumber() > 0){
				GlobalCfg.reset(cfg);
				msg = "success to reload global cfg.\r\n";
			}else{
				msg = "failed to reload global cfg, number shuld be positive";
			}

			logger.info(msg);
			logger.info(cfg);
			return Response.status(200).entity(msg).build();
		} catch (Exception e) {
			String msg = "failed to reload global cfg.\r\n";
			msg += e.getMessage();
			msg += "\r\n";
			logger.error(msg, e);
			return Response.status(400).entity(msg).build();
		}
	}

	@GET
	@Produces(MediaType.APPLICATION_JSON)
	public Response getCfg() {
		GlobalCfg msg = GlobalCfg.getInstance();
		return Response.status(200).entity(msg).build();
	}
}
