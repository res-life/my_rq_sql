package com.yeahmobi.datasystem.query.jersey;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * Created by Administrator on 13-11-30.
 */
@Path("/log")
public class LogLevelService {

	@GET
	@Produces(MediaType.TEXT_PLAIN)
	public Response doGET(@QueryParam("m") String method,
			@QueryParam("l") String level, @QueryParam("c") String app) {

		String res;
		Logger logger = Logger.getLogger("com.yeahmobi.datasystem.query" + "."
				+ app);
		res = app + " -> " + logger.getLevel();
		if ("set".equalsIgnoreCase(method)) {
			logger.setLevel(Level.toLevel(level));
			res += " -> " + logger.getLevel();
		}

		return Response.status(200).entity(res).build();
	}

}
