package com.yeahmobi.datasystem.query.jersey;

/**
 * Created by wanghw 2014.5.29.
 */

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.log4j.Logger;

import com.google.common.io.CharStreams;

/**
 * GET: /inputcheck
 */
@Path("/inputcheck")
public class InputCheckService {

	private static Logger logger = Logger.getLogger(InputCheckService.class);

	@GET
	@Produces(MediaType.TEXT_HTML)
	public Response doGet() {
		ClassLoader classloader = Thread.currentThread()
				.getContextClassLoader();
		InputStream is = classloader.getResourceAsStream("inputcheck.html");
		String data = "No File Query.html In Resources Folder";
		int code = 404;
		if (is != null) {
			try {
				data = CharStreams.toString(new InputStreamReader(is, "UTF-8"));
				code = 200;
			} catch (IOException e) {
				logger.error("", e);
			}
		}
		return Response.status(code).entity(data).build();
	}
}
