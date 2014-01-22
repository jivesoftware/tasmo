/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.jivesoftware.os.tasmo.view.reader.service;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.tasmo.id.ObjectId;
import com.jivesoftware.os.tasmo.id.TenantIdAndCentricId;
import com.jivesoftware.os.tasmo.view.reader.api.ViewFieldVersion;
import com.jivesoftware.os.tasmo.view.reader.api.ViewResponse;
import java.util.List;

public class ViewAsObjectNode implements ViewFormatter<ViewResponse> {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    @Override
    public ViewResponse getView(TenantIdAndCentricId tenantIdAndCentricId, ObjectId viewId, ViewResponse viewResponse,
        List<ViewFieldVersion> viewFieldVersions) {
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
            }
        }
        return viewResponse;
    }

    @Override
    public ViewResponse emptyView() {
        return ViewResponse.notFound();
    }
}
