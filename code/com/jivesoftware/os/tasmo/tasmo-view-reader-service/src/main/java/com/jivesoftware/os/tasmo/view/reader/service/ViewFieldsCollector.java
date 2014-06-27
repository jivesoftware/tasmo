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
import com.jivesoftware.os.jive.utils.id.Id;
import com.jivesoftware.os.jive.utils.id.ObjectId;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.tasmo.model.path.ModelPath;
import com.jivesoftware.os.tasmo.model.path.ModelPathStep;
import com.jivesoftware.os.tasmo.view.reader.api.ViewDescriptor;
import com.jivesoftware.os.tasmo.view.reader.api.ViewResponse;
import com.jivesoftware.os.tasmo.view.reader.service.shared.ViewValue;
import java.io.IOException;
import java.util.List;
import java.util.Set;

public class ViewFieldsCollector {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    private final JsonViewMerger merger;
    private MapTreeNode treeRoot;
    private final long viewMaxSizeInBytes;
    private long viewSizeInBytes = 0;

    public ViewFieldsCollector(JsonViewMerger merger, long viewMaxSizeInBytes) {
        this.merger = merger;
        this.viewMaxSizeInBytes = viewMaxSizeInBytes;
    }

    public boolean add(ViewDescriptor viewDescriptor,
            ModelPath modelPath,
            Id[] modelPathIds,
            String[] viewPathClasses,
            ViewValue viewValue,
            Long timestamp) throws IOException {

        byte[] value = (viewValue == null) ? null : viewValue.getValue();
        viewSizeInBytes += (value == null) ? 0 : value.length;
        if (viewSizeInBytes > viewMaxSizeInBytes) {
            LOG.error("ViewDescriptor:" + viewDescriptor + " is larger than viewMaxReadableBytes:" + viewMaxSizeInBytes);
            return false;
        }

        if (viewValue == null || viewValue.getValue() == null || viewValue.getValue().length == 0) {
            return false;
        }
        ObjectNode valueObject = merger.toObjectNode(viewValue.getValue());
        if (valueObject == null || valueObject.isNull() || valueObject.size() == 0) {
            return false;
        }

        ObjectId[] modelPathInstanceIds = modelPathInstanceIds(modelPathIds, viewPathClasses, modelPath.getPathMembers());

        LOG.debug("Read view path -> with id={} instance ids={} value={} timestamp={}", new Object[]{modelPath.getId(), modelPathIds, viewValue, timestamp});

        if (treeRoot == null) {
            treeRoot = new MapTreeNode(modelPathInstanceIds[0]);
        }
        treeRoot.add(modelPath.getPathMembers().toArray(new ModelPathStep[modelPath.getPathMemberSize()]), modelPathInstanceIds, viewValue, timestamp);
        return true;
    }

    public void done() {
    }

    public ViewResponse getView(Set<Id> canViewTheseIds) throws Exception {
        if (viewSizeInBytes > viewMaxSizeInBytes) {
            return ViewResponse.toLarge();
        }
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
