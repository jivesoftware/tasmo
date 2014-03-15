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
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.tasmo.event.api.ReservedFields;
import com.jivesoftware.os.tasmo.id.ObjectId;
import com.jivesoftware.os.tasmo.id.TenantId;
import com.jivesoftware.os.tasmo.model.ViewBinding;
import com.jivesoftware.os.tasmo.model.path.ModelPath;
import com.jivesoftware.os.tasmo.model.path.ModelPathStep;
import com.jivesoftware.os.tasmo.model.path.ModelPathStepType;
import com.jivesoftware.os.tasmo.view.reader.api.ViewDescriptor;
import com.jivesoftware.os.tasmo.view.reader.api.ViewReader;
import com.jivesoftware.os.tasmo.view.reader.api.ViewReaderException;
import com.jivesoftware.os.tasmo.view.reader.api.ViewResponse;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class ReadTimeViewMaterializer implements ViewReader<ViewResponse> {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    private final ViewModelProvider viewModelProvider;
    private final ReferenceGatherer referenceGatherer;
    private final ValueGatherer valueGatherer;
    private final ViewPermissionChecker viewPermissionChecker;
    private final ObjectMapper mapper;

    public ReadTimeViewMaterializer(ViewModelProvider viewModelProvider, ReferenceGatherer referenceGatherer, ValueGatherer valueGatherer,
        ViewPermissionChecker viewPermissionChecker) {
        this.viewModelProvider = viewModelProvider;
        this.referenceGatherer = referenceGatherer;
        this.valueGatherer = valueGatherer;
        this.viewPermissionChecker = viewPermissionChecker;
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
            if (view.size() == 0) {
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

        ViewAccumulator accumulator = new ViewAccumulator(mapper, viewPermissionChecker);
        Multimap<String, ValueRequest> valueSteps = ArrayListMultimap.create();
        Multimap<String, ReferenceRequest> referenceSteps = ArrayListMultimap.create();

        setInitialId(viewRequest, binding, accumulator);

        boolean treeLevelsRemain = true;
        int level = 0;
        List<ModelPath> paths = binding.getModelPaths();

        while (treeLevelsRemain) {
            treeLevelsRemain = false;

            for (int i = 0; i < paths.size(); i++) {
                ModelPath path = paths.get(i);

                if (level < path.getPathMemberSize()) {
                    treeLevelsRemain = true;

                    ModelPathStep step = path.getPathMembers().get(level);
                    if (ModelPathStepType.value.equals(step.getStepType())) {
                        for (ObjectId sourceId : accumulator.getIdsForPathAndDepth(path.getId(), level)) {
                            valueSteps.put(path.getId(), new ValueRequest(step, sourceId));
                        }
                    } else {
                        for (ObjectId sourceId : accumulator.getIdsForPathAndDepth(path.getId(), level)) {
                            referenceSteps.put(path.getId(), new ReferenceRequest(step, sourceId));
                        }
                    }
                }
            }

            accumulator.addRefResults(referenceGatherer.gatherReferenceResults(viewRequest.getTenantIdAndCentricId(), referenceSteps));
            referenceSteps.clear();
        }

        accumulator.addValueResults(valueGatherer.gatherValueResults(viewRequest.getTenantIdAndCentricId(), valueSteps));

        ObjectNode responseBody = accumulator.formatResults(viewRequest.getTenantId(), viewRequest.getActorId());
        return getView(viewRequest.getTenantId(), viewRequest.getViewId(), ViewResponse.ok(responseBody));

    }

    //TODO always pull deleted field with values. Value result will have fields + deleted
    //TODO detect invisible root id and return FORBIDDEN
    private void setInitialId(ViewDescriptor viewRequest, ViewBinding binding, ViewAccumulator accumulator) {
        Multimap<String, ObjectId> initialIds = ArrayListMultimap.create();
        ObjectId initialObject = viewRequest.getViewId(); //this is wrong - we need the object id and  this doesn't have it yet
        for (ModelPath path : binding.getModelPaths()) {
            initialIds.put(path.getId(), initialObject);
        }
        accumulator.addRefResults(initialIds);
    }
}
