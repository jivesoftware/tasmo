/*
 * Copyright 2014 pete.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.jivesoftware.os.tasmo.view.reader.lib;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.tasmo.event.api.ReservedFields;
import com.jivesoftware.os.tasmo.id.Id;
import com.jivesoftware.os.tasmo.id.ObjectId;
import com.jivesoftware.os.tasmo.id.TenantId;
import com.jivesoftware.os.tasmo.model.ViewBinding;
import com.jivesoftware.os.tasmo.view.reader.api.ViewDescriptor;
import com.jivesoftware.os.tasmo.view.reader.api.ViewReader;
import com.jivesoftware.os.tasmo.view.reader.api.ViewReaderException;
import com.jivesoftware.os.tasmo.view.reader.api.ViewResponse;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 *
 */
public class ReadTimeViewMaterializer implements ViewReader<ViewResponse> {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    private final ViewModelProvider viewModelProvider;
    private final ReferenceGatherer referenceGatherer;
    private final ValueGatherer valueGatherer;
    private final ViewFormatter<ObjectNode> viewFormatter;
    private final ViewPermissionChecker viewPermissionChecker;
    private final ExistenceChecker existenceChecker;
    private final ObjectMapper mapper;

    public ReadTimeViewMaterializer(ViewModelProvider viewModelProvider, ReferenceGatherer referenceGatherer, ValueGatherer valueGatherer,
        ViewFormatter<ObjectNode> viewFormatter, ViewPermissionChecker viewPermissionChecker, ExistenceChecker existenceChecker) {
        this.viewModelProvider = viewModelProvider;
        this.referenceGatherer = referenceGatherer;
        this.valueGatherer = valueGatherer;
        this.viewFormatter = viewFormatter;
        this.viewPermissionChecker = viewPermissionChecker;
        this.existenceChecker = existenceChecker;
        this.mapper = new ObjectMapper();
        mapper.configure(SerializationFeature.INDENT_OUTPUT, true);
    }

    @Override
    public ViewResponse readView(ViewDescriptor viewRequest) throws IOException, ViewReaderException {
        List<ViewResponse> views = readViews(ImmutableList.of(viewRequest));
        return views.isEmpty() ? ViewResponse.notFound() : views.get(0);
    }

    @Override
    public List<ViewResponse> readViews(List<ViewDescriptor> viewRequests) throws IOException, ViewReaderException {
        List<ViewResponse> responses = new ArrayList<>(viewRequests.size());
        for (ViewDescriptor descriptor : viewRequests) {
            try {
                responses.add(buildView(descriptor));
            } catch (Exception ex) {
                LOG.error("Unable to build view for request: " + descriptor, ex);
                responses.add(ViewResponse.error());
            }
        }

        return responses;
    }

    private ViewResponse getView(TenantId tenantId, ObjectId viewId, ViewResponse viewResponse) {
        if (viewResponse.getStatusCode() == ViewResponse.StatusCode.OK) {
            ObjectNode view = viewResponse.getViewBody();
            if (view == null || view.size() == 0) {
                LOG.debug("Retrieved empty view object for view object id {}. Returning null view object", viewId);
                viewResponse = ViewResponse.notFound();
            } else if (view.has(ReservedFields.DELETED) && view.get(ReservedFields.DELETED).booleanValue()) {
                LOG.debug("Encountered deleted view object with id {}. Returning null view object", viewId);
                viewResponse = ViewResponse.notFound();
            } else {
                view.put(ReservedFields.VIEW_CLASS, viewId.getClassName());
                view.put(ReservedFields.TENANT_ID, tenantId.toStringForm());
                //view.put(ReservedFields.USER_ID, tenantIdAndCentricId.getUserId().toStringForm());
            }
        }
        return viewResponse;
    }

    public ViewResponse buildView(ViewDescriptor viewRequest) throws Exception {
        ViewBinding binding = viewModelProvider.getBindingForRequest(viewRequest);
        if (binding == null) {
            LOG.info("No view binding found to process request %s", viewRequest);
            return ViewResponse.error();
        }

        ObjectId initialId = findInitialId(viewRequest, binding);
        ViewAccumulator<ObjectNode> accumulator = new ViewAccumulator<>(initialId, binding.getModelPaths(), viewPermissionChecker, existenceChecker);
        
        Multimap<String, ViewReference> requestsToMake;
        
        while (!(requestsToMake = accumulator.buildNextViewLevel()).isEmpty()) {
            referenceGatherer.gatherReferenceResults(viewRequest.getTenantIdAndCentricId(), requestsToMake);
            accumulator.addRefResults(requestsToMake);
        }
        
        valueGatherer.gatherValueResults(viewRequest.getTenantIdAndCentricId(), accumulator.getViewValues());

        ObjectNode responseBody = accumulator.formatResults(viewRequest.getTenantId(), viewRequest.getActorId(), viewFormatter);
        return getView(viewRequest.getTenantId(), viewRequest.getViewId(), ViewResponse.ok(responseBody));

    }
    
    private ObjectId findInitialId(ViewDescriptor viewDescriptor, ViewBinding binding) throws Exception {
        Id rootId = viewDescriptor.getViewId().getId();
        Set<ObjectId> potentialRoots = new HashSet<>();
        for (String eventClass : binding.getModelPaths().get(0).getRootClassNames()) {
            potentialRoots.add(new ObjectId(eventClass, rootId));
        }
        
        Set<ObjectId> foundRoots = valueGatherer.lookupEventIds(viewDescriptor.getTenantIdAndCentricId(), potentialRoots);
        
        if (foundRoots.size() > 1) {
            LOG.warn("Unexpectedly found more than one root object for view id " + viewDescriptor.getViewId() + " found " + foundRoots);
        }
        
        return foundRoots.isEmpty() ? null : foundRoots.iterator().next();
    }

}
