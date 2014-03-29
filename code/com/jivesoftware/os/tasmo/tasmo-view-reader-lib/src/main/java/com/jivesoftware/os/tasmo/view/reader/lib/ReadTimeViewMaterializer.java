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
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
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
import com.jivesoftware.os.tasmo.view.reader.lib.ViewAccumulator.RootAndPaths;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 *
 */
public class ReadTimeViewMaterializer implements ViewReader<ViewResponse> {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    private final ViewModelProvider viewModelProvider;
    private final ReferenceGatherer referenceGatherer;
    private final ValueGatherer valueGatherer;
    private final ViewFormatterProvider<ObjectNode> viewFormatterProvider;
    private final ViewPermissionChecker viewPermissionChecker;
    private final ExistenceChecker existenceChecker;
    private final ObjectMapper mapper;

    public ReadTimeViewMaterializer(ViewModelProvider viewModelProvider, ReferenceGatherer referenceGatherer, ValueGatherer valueGatherer,
        ViewFormatterProvider<ObjectNode> viewFormatterProvider, ViewPermissionChecker viewPermissionChecker, ExistenceChecker existenceChecker) {
        this.viewModelProvider = viewModelProvider;
        this.referenceGatherer = referenceGatherer;
        this.valueGatherer = valueGatherer;
        this.viewFormatterProvider = viewFormatterProvider;
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
        try {
            return buildViews(viewRequests);
        } catch (Exception ex) {
            throw new ViewReaderException("Unable to process view read request", ex);
        }
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

    public List<ViewResponse> buildViews(List<ViewDescriptor> viewRequests) throws Exception {
        ViewRequestBatch requestBatch = new ViewRequestBatch(viewRequests);

        for (ViewDescriptor descriptor : viewRequests) {
            ViewBinding binding = viewModelProvider.getBindingForRequest(descriptor);
            if (binding == null) {
                LOG.info("No view binding found to process request %s", viewRequests);
                requestBatch.setResponse(descriptor, ViewResponse.error());
            } else {
                requestBatch.setBinding(descriptor, binding);
            }
        }

        findInitialIds(requestBatch);

        ViewAccumulator<ObjectNode> accumulator =
            new ViewAccumulator<>(requestBatch.getPendingRequestPaths(), viewPermissionChecker, existenceChecker);

        Map<ViewDescriptor, Multimap<String, ViewReference>> requestsToMake;

        while (!(requestsToMake = accumulator.buildNextViewLevel()).isEmpty()) {
            referenceGatherer.gatherReferenceResults(requestsToMake);
            accumulator.addRefResults(requestsToMake);
        }

        valueGatherer.gatherValueResults(accumulator.getViewValues(requestBatch.getPendingRequests()));

        for (ViewDescriptor viewRequest : requestBatch.getPendingRequests()) {
            ViewFormatter<ObjectNode> viewFormatter = viewFormatterProvider.createViewFormatter();
            ObjectNode responseBody = accumulator.formatResults(viewRequest, viewFormatter);
            if (accumulator.forbidden(viewRequest)) {
                requestBatch.setResponse(viewRequest, ViewResponse.forbidden());
            } else {
                ViewResponse response = getView(viewRequest.getTenantId(), viewRequest.getViewId(), ViewResponse.ok(responseBody));
                requestBatch.setResponse(viewRequest, response);
            }
        }

        return requestBatch.inOrderResponses(viewRequests);
    }

    private void findInitialIds(ViewRequestBatch requestBatch) throws Exception {

        Map<ViewDescriptor, Set<ObjectId>> allPotentialRoots = new HashMap<>();
        for (ViewDescriptor request : requestBatch.getPendingRequests()) {
            ViewBinding binding = requestBatch.getBinding(request);
            Set<ObjectId> potential = getPotentialRoots(request, binding);
            if (potential.isEmpty()) {
                requestBatch.setResponse(request, ViewResponse.error());
            } else {
                allPotentialRoots.put(request, potential);
            }
        }

        Map<ViewDescriptor, ObjectId> foundRoots = valueGatherer.lookupEventIds(allPotentialRoots);
        for (ViewDescriptor descriptor : requestBatch.getPendingRequests()) {
            ObjectId root = foundRoots.get(descriptor);
            if (root == null) {
                requestBatch.setResponse(descriptor, ViewResponse.notFound());
            } else {
                requestBatch.setRootObject(descriptor, root);
            }
        }
    }

    private Set<ObjectId> getPotentialRoots(ViewDescriptor request, ViewBinding binding) {
        Id rootId = request.getViewId().getId();
        Set<ObjectId> potentialRoots = new HashSet<>();
        for (String eventClass : binding.getModelPaths().get(0).getRootClassNames()) {
            potentialRoots.add(new ObjectId(eventClass, rootId));
        }

        return potentialRoots;

    }

    //state for one in flight batch of view requests
    private static class ViewRequestBatch {

        private final Map<ViewDescriptor, ViewRequestContext> requestContexts = new HashMap<>();

        private ViewRequestBatch(List<ViewDescriptor> requests) {
            for (ViewDescriptor request : requests) {
                requestContexts.put(request, new ViewRequestContext());
            }
        }

        private void setResponse(ViewDescriptor request, ViewResponse response) {
            ViewRequestContext context = Preconditions.checkNotNull(requestContexts.get(request));
            context.response = response;
        }

        private void setBinding(ViewDescriptor request, ViewBinding binding) {
            ViewRequestContext context = Preconditions.checkNotNull(requestContexts.get(request));
            context.viewBinding = binding;
        }

        private ViewBinding getBinding(ViewDescriptor request) {
            ViewRequestContext context = Preconditions.checkNotNull(requestContexts.get(request));
            return context.viewBinding;
        }

        private void setRootObject(ViewDescriptor request, ObjectId viewRoot) {
            ViewRequestContext context = Preconditions.checkNotNull(requestContexts.get(request));
            context.viewRoot = viewRoot;
        }

        private Iterable<ViewDescriptor> getPendingRequests() {
            return Iterables.filter(requestContexts.keySet(), new Predicate<ViewDescriptor>() {
                @Override
                public boolean apply(ViewDescriptor t) {
                    return requestContexts.get(t).response == null;
                }
            });
        }

        private Map<ViewDescriptor, RootAndPaths> getPendingRequestPaths() {
            Map<ViewDescriptor, RootAndPaths> allPaths = new HashMap<>();
            for (ViewDescriptor descriptor : getPendingRequests()) {
                ViewRequestContext context = requestContexts.get(descriptor);
                allPaths.put(descriptor, new RootAndPaths(context.viewRoot, context.viewBinding.getModelPaths()));
            }

            return allPaths;
        }

        private List<ViewResponse> inOrderResponses(List<ViewDescriptor> viewRequests) {
            List<ViewResponse> responses = new ArrayList<>();
            for (ViewDescriptor request : viewRequests) {
                responses.add(requestContexts.get(request).response);
            }

            return responses;
        }
    }

    private static class ViewRequestContext {

        ViewDescriptor viewDescriptor;
        ViewBinding viewBinding;
        ObjectId viewRoot;
        ViewResponse response;
    }
}
