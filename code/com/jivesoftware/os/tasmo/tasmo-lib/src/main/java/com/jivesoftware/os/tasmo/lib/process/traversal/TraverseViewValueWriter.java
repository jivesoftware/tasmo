package com.jivesoftware.os.tasmo.lib.process.traversal;

import com.jivesoftware.os.jive.utils.id.Id;
import com.jivesoftware.os.jive.utils.id.ObjectId;
import com.jivesoftware.os.jive.utils.id.TenantIdAndCentricId;
import com.jivesoftware.os.tasmo.lib.process.WrittenEventContext;
import com.jivesoftware.os.tasmo.lib.write.PathId;
import com.jivesoftware.os.tasmo.lib.write.ViewField;
import com.jivesoftware.os.tasmo.model.path.ModelPath;
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
    private final ModelPath modelPath;
    private final long modelPathIdHashcode;

    public TraverseViewValueWriter(String viewIdFieldName, String viewClassName, ModelPath modelPath, long modelPathIdHashcode) {
        this.viewIdFieldName = viewIdFieldName;
        this.viewClassName = viewClassName;
        this.modelPath = modelPath;
        this.modelPathIdHashcode = modelPathIdHashcode;
    }

    @Override
    public void process(final TenantIdAndCentricId globalCentricId,
        final TenantIdAndCentricId userCentricId,
        WrittenEventContext writtenEventContext,
        PathTraversalContext pathTraversalContext,
        PathContext pathContext,
        LeafContext leafContext,
        PathId from,
        StepStream streamTo) throws Exception {

        Id viewId = buildAlternateViewId(writtenEventContext.getEvent());
        if (viewId == null && from != null) {
            ObjectId objectId = from.getObjectId();
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
            ViewField.ViewFieldChangeType type = ViewField.ViewFieldChangeType.add;
            if (pathTraversalContext.isRemovalContext()) {
                type = ViewField.ViewFieldChangeType.remove;
            }

            if (viewId == null) {
                viewId = Id.NULL; // HACK
            }

            ViewField update = new ViewField(writtenEventContext.getEventId(),
                writtenEventContext.getActorId(),
                type,
                new ObjectId(viewClassName, viewId),
                modelPath,
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
        return "TraverseViewValueWriter{"
            + "viewIdFieldName=" + viewIdFieldName
            + ", viewClassName=" + viewClassName
            + ", modelPathId=" + modelPathIdHashcode
            + '}';
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
