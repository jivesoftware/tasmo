package com.jivesoftware.os.tasmo.service;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Joiner;
import com.google.inject.Singleton;
import com.jivesoftware.os.jive.utils.jaxrs.util.ResponseHelper;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.tasmo.lib.TasmoProcessingStats;
import com.jivesoftware.os.tasmo.lib.report.TasmoEdgeReport;
import java.util.List;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Singleton
@Path("/materializer")
public class MaterializerServiceEndpoints {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    @Context
    EventConvertingCallbackStream ingressWrittenEvents;
    @Context
    TasmoEdgeReport tasmoEdgeReport;
    @Context
    TasmoProcessingStats tasmoProcessingStats;

    @POST
    @Consumes("application/json")
    @Path("/writtenEvents")
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
    @Path("/report/txt")
    public Response hello() {
        String report = Joiner.on("\n").join(tasmoEdgeReport.getTextReport());
        return Response.ok(report, MediaType.TEXT_PLAIN).build();
    }

    @GET
    @Path("/stats/txt")
    public Response stats() {
        return Response.ok(tasmoProcessingStats.getTextReport(), MediaType.TEXT_PLAIN).build();
    }
}
