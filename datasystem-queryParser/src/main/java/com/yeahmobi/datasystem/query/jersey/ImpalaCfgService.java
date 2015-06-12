package com.yeahmobi.datasystem.query.jersey;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.yeahmobi.datasystem.query.meta.ImpalaCfg;

@Path("/impala_cfg")
public class ImpalaCfgService {

	@POST
	@Consumes(MediaType.APPLICATION_JSON)
	public Response setDimension(ImpalaCfg cfgs) {
		try {
			ImpalaCfg.reset(cfgs);
			return Response.status(200).entity("successfully").build();
		} catch (Exception e) {
			String msg = "failed to set impala_cfg.\r\n";
			msg += e.getMessage();
			msg += "\r\n";
			return Response.status(400).entity(msg).build();
		}
	}

	@GET
	@Produces(MediaType.APPLICATION_JSON)
	public Response getDimention() {
		return Response.status(200).entity(ImpalaCfg.getInstance()).build();
	}
}
