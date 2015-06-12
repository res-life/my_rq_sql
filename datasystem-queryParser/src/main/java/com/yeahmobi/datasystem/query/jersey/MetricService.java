package com.yeahmobi.datasystem.query.jersey;

import java.util.Map;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.yeahmobi.datasystem.query.meta.MetricAggTable;
import com.yeahmobi.datasystem.query.meta.MetricDetail;

@Path("/metric")
public class MetricService {

	@POST
	@Consumes(MediaType.APPLICATION_JSON)
	public Response setDimension(MetricAggTable table) {
		try {

			MetricDimentionManager.setMetric(table);
			return Response.status(200).entity("successfully").build();
		} catch (Exception e) {
			String msg = "failed to set metric.\r\n";
			msg += e.getMessage();
			msg += "\r\n";
			return Response.status(400).entity(msg).build();
		}
	}

	@GET
	@Path("{id}")
	@Produces(MediaType.APPLICATION_JSON)
	public Response getDimention(@PathParam("id") String id) {
		Map<String, MetricDetail> talbe = MetricDimentionManager.getMetric(id).getTable();
		return Response.status(200).entity(talbe).build();
	}
}
