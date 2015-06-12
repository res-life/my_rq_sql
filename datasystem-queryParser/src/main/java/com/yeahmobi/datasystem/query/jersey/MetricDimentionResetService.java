package com.yeahmobi.datasystem.query.jersey;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Path("/reset_dimension_metric")
public class MetricDimentionResetService {

	@GET
	@Produces(MediaType.TEXT_PLAIN)
	public Response doGet() {
		MetricDimentionManager.reset();
		return Response.status(200).entity("ok").build();
	}
	
	
}