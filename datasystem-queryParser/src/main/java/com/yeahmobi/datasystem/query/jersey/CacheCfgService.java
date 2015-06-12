package com.yeahmobi.datasystem.query.jersey;

/**
 * Created by yangxu on 5/5/14.
 */

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.log4j.Logger;

import com.yeahmobi.datasystem.query.meta.CacheCfg;

/**
 * cache全局设置
 */
@Path("/cache")
public class CacheCfgService {

	private static Logger logger = Logger.getLogger(CacheCfgService.class);

	/**
	 * reload the cache cfg <br>
	 * TODO need password to protect? <br>
	 * 
	 * @return
	 */
	@POST
	@Consumes(MediaType.APPLICATION_JSON)
	public Response reloadCacheCfg(CacheCfg cfg) {
		try {
			if (cfg.isClearL1()) {
				// TODO refactor
			}

			if (cfg.isClearL2()) {
				// TODO refactor
			}

			// 重新设置全局变量
			CacheCfg.reset(cfg);

			String msg = "success to reload cache cfg.\r\n";
			logger.info(msg);
			logger.info(cfg);
			return Response.status(200).entity(msg).build();
		} catch (Exception e) {
			String msg = "failed to reload cache cfg.\r\n";
			logger.error(msg, e);
			return Response.status(400).entity(msg).build();
		}
	}

	@GET
	@Produces(MediaType.APPLICATION_JSON)
	public Response getCfg() {
		CacheCfg msg = CacheCfg.getInstance();
		return Response.status(200).entity(msg).build();
	}
}
