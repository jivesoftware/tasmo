package com.jivesoftware.os.tasmo.view.reader.service;

import com.google.inject.Singleton;
import com.jivesoftware.os.jive.utils.jaxrs.util.ResponseHelper;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.tasmo.view.reader.api.ViewDescriptor;
import com.jivesoftware.os.tasmo.view.reader.api.ViewReader;
import com.jivesoftware.os.tasmo.view.reader.api.ViewReaderAPIEndpoints;
import com.jivesoftware.os.tasmo.view.reader.api.ViewReaderException;
import com.jivesoftware.os.tasmo.view.reader.api.ViewResponse;
import java.io.IOException;
import java.util.List;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;

@Singleton
@Path(ViewReaderAPIEndpoints.VIEW_READER_ENDPOINT_PREFIX)
public class ViewReaderServiceEndpoints {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    private final ViewReader viewReader;

    public ViewReaderServiceEndpoints(@Context ViewReader viewReader) {
        this.viewReader = viewReader;
    }

    @POST
    @Consumes("application/json")
    @Produces("application/json")
    @Path(ViewReaderAPIEndpoints.VIEW_READER_GET_ENDPOINT)
    public List<ViewResponse> getTheseIds(List<ViewDescriptor> viewDescriptors) {
        long start = System.currentTimeMillis();
        try {
            return viewReader.readViews(viewDescriptors);
        } catch (IOException | ViewReaderException e) {
            LOG.error("failed to get:" + viewDescriptors, e);
            throw new WebApplicationException(ResponseHelper.INSTANCE.errorResponse("failed to get views for ids.", e));
        } finally {
            LOG.info("/get " + viewDescriptors + " took " + (System.currentTimeMillis() - start));
        }
    }

}
