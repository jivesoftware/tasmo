package com.jivesoftware.os.tasmo.view.notification.api;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.jivesoftware.os.tasmo.id.BaseView;
import com.jivesoftware.os.tasmo.view.notification.api.ViewNotification;
import com.jivesoftware.os.tasmo.view.notification.api.ViewNotification;

/**
 *
 */
public class ViewChange<V extends BaseView<?>> {

    private final ViewNotification viewNotification;
    private final ObjectNode objectNode;
    private final V view;

    public ViewChange(ViewNotification viewNotification, ObjectNode objectNode, V view) {
        this.viewNotification = viewNotification;
        this.objectNode = objectNode;
        this.view = view;
    }

    public ViewNotification getViewNotification() {
        return viewNotification;
    }

    public ObjectNode getObjectNode() {
        return objectNode;
    }

    public V getView() {
        return view;
    }
}
