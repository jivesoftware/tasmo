/*
 * $Revision$
 * $Date$
 *
 * Copyright (C) 1999-$year$ Jive Software. All rights reserved.
 *
 * This software is the proprietary information of Jive Software. Use is subject to license terms.
 */
package com.jivesoftware.os.tasmo.lib.process.traversal;

import com.jivesoftware.os.jive.utils.id.Id;
import com.jivesoftware.os.jive.utils.id.ObjectId;
import com.jivesoftware.os.jive.utils.id.TenantIdAndCentricId;
import com.jivesoftware.os.tasmo.lib.process.WrittenEventContext;
import com.jivesoftware.os.tasmo.lib.write.PathId;
import com.jivesoftware.os.tasmo.lib.write.ViewFieldChange;
import com.jivesoftware.os.tasmo.model.process.WrittenEvent;
import com.jivesoftware.os.tasmo.model.process.WrittenInstance;
import java.io.IOException;
import java.util.Objects;

/**
 *
 */
public class TraverseViewValueWriter implements StepTraverser {

    private final String viewIdFieldName;
    private final String viewClassName;
    private final long modelPathIdHashcode;

    public TraverseViewValueWriter(String viewIdFieldName, String viewClassName, long modelPathIdHashcode) {
        this.viewIdFieldName = viewIdFieldName;
        this.viewClassName = viewClassName;
        this.modelPathIdHashcode = modelPathIdHashcode;
    }

    @Override
    public void process(final TenantIdAndCentricId tenantIdAndCentricId,
            WrittenEventContext writtenEventContext,
            PathTraversalContext pathTraversalContext,
            PathContext pathContext,
            LeafContext leafContext,
            PathId from,
            StepStream streamTo) throws Exception {

        ObjectId objectId = from.getObjectId();
        Id viewId = buildAlternateViewId(writtenEventContext.getEvent());
        if (viewId == null) {
            viewId = objectId.getId();
        }

        writeViewFields(writtenEventContext, pathTraversalContext, pathContext, leafContext, viewClassName, modelPathIdHashcode, viewId);
    }

    public void writeViewFields(WrittenEventContext writtenEventContext,
            PathTraversalContext pathTraversalContext,
            PathContext pathContext,
            LeafContext leafContext,
            String viewClassName,
            long modelPathIdHashcode,
            Id viewId) throws IOException {

        byte[] leafAsBytes = leafContext.toBytes();
        if (leafAsBytes != null) {
            WrittenEvent writtenEvent = writtenEventContext.getEvent();
            ViewFieldChange update = new ViewFieldChange(writtenEvent.getEventId(),
                    writtenEvent.getActorId(),
                    (pathTraversalContext.isRemovalContext()) ? ViewFieldChange.ViewFieldChangeType.remove : ViewFieldChange.ViewFieldChangeType.add, // uck
                    new ObjectId(viewClassName, viewId),
                    modelPathIdHashcode,
                    pathContext.copyOfModelPathInstanceIds(),
                    pathContext.copyOfVersions(),
                    pathContext.copyOfModelPathTimestamps(),
                    leafAsBytes,
                    pathTraversalContext.getThreadTimestamp());
            pathTraversalContext.addChange(update);
        }
    }

    protected Id buildAlternateViewId(WrittenEvent writtenEvent) throws IllegalStateException {
        Id alternateViewId = null;
        if (viewIdFieldName != null) {
            WrittenInstance payload = writtenEvent.getWrittenInstance();

            if (payload.hasField(viewIdFieldName)) {
                try {
                    // We are currently only supporting one ref, but with light change we could support a list of refs. Should we?
                    ObjectId objectId = payload.getReferenceFieldValue(viewIdFieldName);
                    if (objectId != null) {
                        alternateViewId = objectId.getId();
                    }
                    alternateViewId = writtenEvent.getWrittenInstance().getIdFieldValue(viewIdFieldName);
                } catch (Exception x) {
                    throw new IllegalStateException("instanceClassName:" + payload.getInstanceId().getClassName() + " requires that field:"
                            + viewIdFieldName
                            + " always be present and of type ObjectId. Please check that you have marked this field as mandatory.", x);
                }
            } else {
                throw new IllegalStateException("instanceClassName:" + payload.getInstanceId().getClassName() + " requires that field:"
                        + viewIdFieldName
                        + " always be present and of type ObjectId. Please check that you have marked this field as mandatory.");
            }
        }
        return alternateViewId;
    }

    @Override
    public String toString() {
        return "TraverseViewValueWriter{" + "viewIdFieldName=" + viewIdFieldName + ", viewClassName=" + viewClassName + ", modelPathId=" + modelPathIdHashcode + '}';
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 89 * hash + Objects.hashCode(this.viewIdFieldName);
        hash = 89 * hash + Objects.hashCode(this.viewClassName);
        hash = 89 * hash + Objects.hashCode(this.modelPathIdHashcode);
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final TraverseViewValueWriter other = (TraverseViewValueWriter) obj;
        if (!Objects.equals(this.viewIdFieldName, other.viewIdFieldName)) {
            return false;
        }
        if (!Objects.equals(this.viewClassName, other.viewClassName)) {
            return false;
        }
        if (!Objects.equals(this.modelPathIdHashcode, other.modelPathIdHashcode)) {
            return false;
        }
        return true;
    }

}
