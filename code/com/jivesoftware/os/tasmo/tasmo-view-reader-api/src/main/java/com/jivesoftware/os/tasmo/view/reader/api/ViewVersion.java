/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.jivesoftware.os.tasmo.view.reader.api;

import com.jivesoftware.os.jive.utils.id.ObjectId;
import java.util.List;

public class ViewVersion {

    private final ObjectId viewId;
    private final String viewObjectId;
    private final List<ViewFieldVersion> fieldVersions;

    public ViewVersion(ObjectId viewId, String viewObjectId, List<ViewFieldVersion> fieldVersions) {
        this.viewId = viewId;
        this.viewObjectId = viewObjectId;
        this.fieldVersions = fieldVersions;
    }

    public ObjectId getViewId() {
        return viewId;
    }

    public String getViewObjectId() {
        return viewObjectId;
    }

    public List<ViewFieldVersion> getFieldVersions() {
        return fieldVersions;
    }

    @Override
    public String toString() {
        return "ViewVersion{" + "viewId=" + viewId + ", viewObjectId=" + viewObjectId + ", fieldVersions=" + fieldVersions + '}';
    }
}
