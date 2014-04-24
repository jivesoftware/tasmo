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
import com.jivesoftware.os.tasmo.view.reader.api.ViewResponse;
import java.io.IOException;
import java.util.List;
import java.util.Set;

public class ViewFieldsCollector {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    private final JsonViewMerger merger;
    private MapTreeNode treeRoot;

    public ViewFieldsCollector(JsonViewMerger merger) {
        this.merger = merger;
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

        if (treeRoot == null) {
            treeRoot = new MapTreeNode(modelPathInstanceIds[0]);
        }
        treeRoot.add(modelPath.getPathMembers().toArray(new ModelPathStep[modelPath.getPathMemberSize()]), modelPathInstanceIds, value, timestamp);
        return true;
    }

    public void done() {
    }

    ViewResponse getView(Set<Id> canViewTheseIds) throws Exception {
        if (treeRoot == null) {
            return ViewResponse.notFound();
        }
        if (!canViewTheseIds.contains(treeRoot.getObjectId().getId())) {
            return ViewResponse.forbidden();
        }
        return ViewResponse.ok((ObjectNode) treeRoot.merge(merger, canViewTheseIds));
    }

    private ObjectId[] modelPathInstanceIds(Id[] modelPathIds, String[] viewPathClasses, List<ModelPathStep> modelPathMembers) {
        ObjectId[] objectIds = new ObjectId[modelPathIds.length];

        for (int idx = 0; idx < modelPathMembers.size(); idx++) {
            objectIds[idx] = new ObjectId(viewPathClasses[idx], modelPathIds[idx]);
        }

        return objectIds;
    }
}
