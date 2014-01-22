/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.jivesoftware.os.tasmo.view.reader.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.tasmo.id.ObjectId;
import com.jivesoftware.os.tasmo.id.TenantIdAndCentricId;
import com.jivesoftware.os.tasmo.view.reader.api.VersionedView;
import com.jivesoftware.os.tasmo.view.reader.api.ViewFieldVersion;
import com.jivesoftware.os.tasmo.view.reader.api.ViewResponse;
import com.jivesoftware.os.tasmo.view.reader.api.ViewVersion;
import java.util.List;

public class ViewAsVersionedView implements ViewFormatter<VersionedView<ViewResponse>> {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    @Override
    public VersionedView<ViewResponse> getView(TenantIdAndCentricId tenantIdAndCentricId,
        ObjectId viewId, ViewResponse viewResponse, List<ViewFieldVersion> viewFieldVersions) {
        ViewVersion viewVersion = null;
        if (viewResponse.getStatusCode() == ViewResponse.StatusCode.OK) {
            ObjectNode view = viewResponse.getViewBody();
            if (view.size() == 0) {
                LOG.debug("Retrieved empty view object for view object id {}. Returning null view object", viewId);
                viewResponse = ViewResponse.notFound();
            } else if (view.has(FieldConstants.DELETED) && view.get(FieldConstants.DELETED).booleanValue()) {
                LOG.debug("Encountered deleted view object with id {}. Returning null view object", viewId);
                viewResponse = ViewResponse.notFound();
            } else {
                view.put(FieldConstants.VIEW_CLASS, viewId.getClassName());
                view.put(FieldConstants.TENANT_ID, tenantIdAndCentricId.getTenantId().toStringForm());
                //view.put(ReservedFields.USER_ID, tenantIdAndCentricId.getUserId().toStringForm());

                JsonNode objectIdNode = view.get(FieldConstants.VIEW_OBJECT_ID);
                if (objectIdNode == null || !objectIdNode.isTextual()) {
                    throw new IllegalStateException("Expected text node for field " + FieldConstants.VIEW_OBJECT_ID + " in view object");
                }
                String viewObjectId = objectIdNode.asText();
                viewVersion = new ViewVersion(viewId, viewObjectId, viewFieldVersions);
            }
        }
        return new VersionedViewImpl(viewResponse, viewVersion);
    }

    @Override
    public VersionedView emptyView() {
        return new VersionedViewImpl(ViewResponse.notFound(), null);
    }

    private static class VersionedViewImpl implements VersionedView<ViewResponse> {
        private final ViewResponse viewResponse;
        private final ViewVersion viewVersion;

        private VersionedViewImpl(ViewResponse viewResponse, ViewVersion viewVersion) {
            this.viewResponse = viewResponse;
            this.viewVersion = viewVersion;
        }

        @Override
        public ViewResponse getView() {
            return viewResponse;
        }

        @Override
        public ViewVersion getVersion() {
            return viewVersion;
        }
    }
}
