/*
 * $Revision$
 * $Date$
 *
 * Copyright (C) 1999-$year$ Jive Software. All rights reserved.
 *
 * This software is the proprietary information of Jive Software. Use is subject to license terms.
 */
package com.jivesoftware.os.tasmo.view.reader.service;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.tasmo.id.Id;
import com.jivesoftware.os.tasmo.id.ObjectId;
import com.jivesoftware.os.tasmo.model.path.ModelPath;
import com.jivesoftware.os.tasmo.model.path.ModelPathStep;
import com.jivesoftware.os.tasmo.view.reader.api.ViewFieldVersion;
import com.jivesoftware.os.tasmo.view.reader.api.ViewResponse;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

public class ViewFieldsCollector {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    private final JsonViewMerger merger;
    private final ObjectNode viewObject;
    private final List<ViewFieldVersion> viewValueFieldVersions;
    private final List<ViewFieldCollector> collectedViewFieldCollectors;
    private String currentModelPathId;
    private String[] currentViewPathClasses;
    private ViewFieldCollector activeViewFieldCollector;

    public ViewFieldsCollector(JsonViewMerger merger) {
        this.merger = merger;
        this.viewObject = merger.createObjectNode();
        this.viewValueFieldVersions = new ArrayList<>();
        this.collectedViewFieldCollectors = new ArrayList<>();
    }

    boolean add(ModelPath modelPath, Id[] modelPathIds, String[] viewPathClasses, String value, Long timestamp) throws IOException {

        if (value == null || value.isEmpty()) {
            return false;
        }
        ObjectNode valueObject = merger.toObjectNode(value);
        if (valueObject == null || valueObject.isNull() || valueObject.size() == 0) {
            return false;
        }

        ObjectId[] modelPathInstanceIds = modelPathInstanceIds(modelPathIds, viewPathClasses, modelPath.getPathMembers());

        LOG.debug("Read view path -> with id={} instance ids={} value={} timestamp={}", new Object[]{modelPath.getId(), modelPathIds, value, timestamp});

        detectAndHandleFieldBoundary(modelPath, viewPathClasses);
        viewValueFieldVersions.add(new ViewFieldVersion(modelPathIds, currentModelPathId, timestamp));
        activeViewFieldCollector.collect(modelPathInstanceIds, value, timestamp);
        return true;
    }

    public void done() {
        collectViewField();
    }

    ViewResponse getView(Set<Id> canViewTheseIds) throws Exception {
        try {
            for (ViewFieldCollector viewFieldCollector : collectedViewFieldCollectors) {
                ObjectNode result = viewFieldCollector.result(canViewTheseIds);
                if (result != null) {
                    merger.merge(viewObject, result);
                }
            }
            return ViewResponse.ok(viewObject);
        } catch (UnauthorizedException e) {
            return ViewResponse.forbidden();
        }
    }

    List<ViewFieldVersion> getViewValueFieldVersions() {
        return viewValueFieldVersions;
    }

    private void detectAndHandleFieldBoundary(ModelPath modelPath, String[] viewPathClasses) {
        if (isFieldBoundary(modelPath, viewPathClasses)) {
            //this indicates a new view field being acted upon
            collectViewField();
            currentModelPathId = modelPath.getId();
            currentViewPathClasses = viewPathClasses;
            activeViewFieldCollector = new ViewFieldCollector(merger, modelPath, viewPathClasses);
        }
    }

    private boolean isFieldBoundary(ModelPath modelPath, String[] viewPathClasses) {
        return !modelPath.getId().equals(currentModelPathId) || !Arrays.equals(currentViewPathClasses, viewPathClasses);
    }

    private void collectViewField() {
        if (activeViewFieldCollector == null) {
            return;
        }
        collectedViewFieldCollectors.add(activeViewFieldCollector);
    }

    private ObjectId[] modelPathInstanceIds(Id[] modelPathIds, String[] viewPathClasses, List<ModelPathStep> modelPathMembers) {
        ObjectId[] objectIds = new ObjectId[modelPathIds.length];

        for (int idx = 0; idx < modelPathMembers.size(); idx++) {
            ModelPathStep member = modelPathMembers.get(idx);

            String className;

            if (member.getStepType().isBackReferenceType()) {
                className = viewPathClasses[idx + 1];
            } else {
                className = viewPathClasses[idx];
            }

            objectIds[idx] = new ObjectId(className, modelPathIds[idx]);
        }

        return objectIds;
    }
}
