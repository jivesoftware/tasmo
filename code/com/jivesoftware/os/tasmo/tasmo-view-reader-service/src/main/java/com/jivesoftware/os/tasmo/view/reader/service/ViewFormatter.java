/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.jivesoftware.os.tasmo.view.reader.service;

import com.jivesoftware.os.tasmo.id.ObjectId;
import com.jivesoftware.os.tasmo.id.TenantIdAndCentricId;
import com.jivesoftware.os.tasmo.view.reader.api.ViewResponse;

/** @author jonathan.colt */
public interface ViewFormatter<V> {

    V getView(TenantIdAndCentricId tenantId, ObjectId viewId, ViewResponse viewResponse);

    V emptyView();
}
