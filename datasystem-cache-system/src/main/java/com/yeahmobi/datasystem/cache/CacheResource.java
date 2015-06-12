package com.yeahmobi.datasystem.cache;

import java.util.LinkedHashMap;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Path("/caches")
public class CacheResource {

	@GET
	@Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
	public CacheRecord getIt() {

		LinkedHashMap<String, DbFieldType> tableFields = new LinkedHashMap<>();
		return CacheRecord.builder().id("1").dataSource("d").query("q").cacheStatus(CacheStatus.NOT_CACHE)
				.createTime(1).isFullData("true").resultTable("r").tableFields(tableFields).timeoutTime(10).build();
	}

	@POST
	@Consumes(MediaType.APPLICATION_JSON)
	public Response po(CacheRecord record) {
		System.out.println(record);
		return Response.status(200).entity("abc").build();

	}
}
