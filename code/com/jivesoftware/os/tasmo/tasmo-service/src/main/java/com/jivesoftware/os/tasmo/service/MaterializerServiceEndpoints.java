package com.jivesoftware.os.tasmo.service;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.inject.Singleton;
import com.jivesoftware.os.jive.utils.jaxrs.util.ResponseHelper;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.tasmo.lib.TasmoBlacklist;
import com.jivesoftware.os.tasmo.lib.TasmoProcessingStats;
import java.util.List;
import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Singleton
@Path ("/materializer")
public class MaterializerServiceEndpoints {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    @Context
    EventConvertingCallbackStream ingressWrittenEvents;

    @Context
    TasmoProcessingStats tasmoProcessingStats;

    @Context
    TasmoBlacklist tasmoBlacklist;

    @POST
    @Consumes ("application/json")
    @Path ("/writtenEvents")
    public Response writtenEvents(List<ObjectNode> events) {
        try {
            LOG.startTimer("writeEvents");
            LOG.inc("ingressed>total", events.size());
            for (ObjectNode event : events) {
                // TODO ensure doneYet tracking is disabled.
            }
            ingressWrittenEvents.callback(events);
            LOG.inc("ingressed>success", events.size());
            return ResponseHelper.INSTANCE.jsonResponse("success");
        } catch (Exception x) {
            LOG.inc("ingressed>errors");
            LOG.error("failed to ingress because:", x);
            return ResponseHelper.INSTANCE.errorResponse(null, x);
        } finally {
            LOG.stopTimer("writeEvents");
        }
    }

    @GET
    @Path ("/stats/txt")
    public Response stats() {
        return Response.ok(tasmoProcessingStats.getTextReport(), MediaType.TEXT_PLAIN).build();
    }

    @GET
    @Path ("/backlisted/events/clear")
    public Response clearBlacklistedEventIds() {
        tasmoBlacklist.clear();
        return Response.ok("done", MediaType.TEXT_PLAIN).build();
    }

    @GET
    @Path ("/blacklist/event")
    public Response blacklistEvent(@QueryParam ("eventId") @DefaultValue ("-1") long eventId) {
        tasmoBlacklist.blacklistEventId(eventId);
        return Response.ok("done", MediaType.TEXT_PLAIN).build();
    }


    @GET
    @Path ("/whitelist/event")
    public Response whitelist(@QueryParam ("eventId") @DefaultValue ("-1") long eventId) {
        tasmoBlacklist.whitelistEventId(eventId);
        return Response.ok("done", MediaType.TEXT_PLAIN).build();
    }
}
