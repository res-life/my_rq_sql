package com.yeahmobi.datasystem.query.jersey;

/**
 * Created by yangxu on 5/5/14.
 */

import java.util.concurrent.TimeUnit;

import javax.ws.rs.DefaultValue;
import javax.ws.rs.FormParam;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.log4j.Logger;
import org.glassfish.jersey.server.ManagedAsync;

import scala.concurrent.Future;
import scala.concurrent.duration.Duration;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.dispatch.OnComplete;
import akka.pattern.Patterns;
import akka.util.Timeout;

import com.google.common.base.Stopwatch;
import com.google.common.base.Strings;
import com.yeahmobi.datasystem.query.StatementType;
import com.yeahmobi.datasystem.query.akka.QueryConfig;
import com.yeahmobi.datasystem.query.meta.MsgType;
import com.yeahmobi.datasystem.query.meta.Results;


/**
 * GET: /report?report_param=xxx[&test=true] POST: /report?[&test=true] --data
 * report_param=xxx
 */
@Path("/report")
public class ReportService {

    private static Logger logger = Logger.getLogger(ReportService.class);

    @Context
    ActorSystem actorSystem;
    
    @Context
    QueryConfig cfg;

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @ManagedAsync
    public void doGet(@DefaultValue("false") @QueryParam("test") boolean test,  @DefaultValue("DRUID") @QueryParam("queryType") String queryType, @DefaultValue("plain") @QueryParam("style") String style,
            @QueryParam("report_param") String param, @Suspended final AsyncResponse res) {
        doPost(test, queryType, style, param, res);
    }

    @POST
    @Produces(MediaType.APPLICATION_JSON)
    @ManagedAsync
    public void doPost(@DefaultValue("false") @QueryParam("test") boolean test, @DefaultValue("DRUID") @QueryParam("queryType") String queryType, @DefaultValue("plain") @QueryParam("style") final String style,
            @FormParam("report_param") final String param, @Suspended final AsyncResponse res) {

        if (Strings.isNullOrEmpty(param)) {
            res.resume(Response.ok().entity(Results.NullParam).build());
            return;
        }

        final Stopwatch stopwatch = Stopwatch.createStarted();

        logger.info("query begin [" + param + "]");

        // forward to akka actor to handle
        ActorRef queryActor = actorSystem.actorFor("/user/QueryRouter");

        Timeout timeout = new Timeout(Duration.create(cfg.getTimeout(), TimeUnit.SECONDS));

        StatementType type = StatementType.valueOf(queryType);
        Future<Object> future = Patterns.ask(queryActor, new ReportServiceRequest(param, style, type), timeout);

        future.onComplete(new OnComplete<Object>() {

            public void onComplete(Throwable failure, Object result) {

                if (failure != null) {

                	// time out exception occurs
                    if (failure.getMessage() != null) {
                        logger.error(failure.getMessage(), failure);
                    }
                    
                    // mark as timeout error
                    res.resume(Response.ok().entity(Results.TimeOut).build());

                } else {
                	ReportServiceResult outerResult = (ReportServiceResult) result;
                	Object ret = outerResult.getResult();

                	if(outerResult.getMsgType().equals(MsgType.fail)){
                		// 发生异常
                		res.resume(Response.ok().entity(ret).build());
                	}else{
                		// 正常返回
                    	res.resume(Response.ok().entity(ret).build());
                	}
                }

                logger.info("query cost " + stopwatch.elapsed(TimeUnit.MILLISECONDS) + " query is " + param);
            }
        }, actorSystem.dispatcher());

    }

}
