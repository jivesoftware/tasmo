/*
 * $Revision$
 * $Date$
 *
 * Copyright (C) 1999-$year$ Jive Software. All rights reserved.
 *
 * This software is the proprietary information of Jive Software. Use is subject to license terms.
 */
package com.jivesoftware.os.tasmo.configuration;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.jivesoftware.os.tasmo.event.api.ReservedFields;
import java.util.Iterator;
import java.util.Objects;

public class ViewModel {

    private final String viewClassName;
    private final boolean idCentric;
    private final boolean notifiable;
    private final String viewIdFieldName;
    private final ViewObject viewObject;

    private ViewModel(String viewClassName, boolean idCentric, boolean notifiable, String viewIdFieldName, ViewObject viewObject) {
        this.viewClassName = viewClassName;
        this.idCentric = idCentric;
        this.notifiable = notifiable;
        this.viewIdFieldName = viewIdFieldName;
        this.viewObject = viewObject;
    }

    public String getViewClassName() {
        return viewClassName;
    }

    public boolean isIdCentric() {
        return idCentric;
    }

    public boolean isNotifiable() {
        return notifiable;
    }

    public String getViewIdFieldName() {
        return viewIdFieldName;
    }

    public ViewObject getViewObject() {
        return viewObject;
    }

    public static ViewConfigurationBuilder builder(ObjectNode exampleViewNode) {
        return new ViewConfigurationBuilder(exampleViewNode);
    }

    public static class ViewConfigurationBuilder {

        private final ObjectNode viewConfigurationNode;

        private ViewConfigurationBuilder(ObjectNode viewConfigurationNode) {
            this.viewConfigurationNode = viewConfigurationNode;
        }

        public ViewModel build() {
            String viewClassName = null;
            boolean idCentric = false;
            boolean notifiable = false;
            String viewIdFieldName = null;
            ViewObject viewObject = null;
            for (Iterator<String> it = viewConfigurationNode.fieldNames(); it.hasNext();) {
                String fieldName = it.next();
                if (fieldName.equals("idCentric")) {
                    idCentric = true;
                }
                if (fieldName.equals("notifiable")) {
                    notifiable = true;
                }
                if (fieldName.equals("viewIdFieldName")) {
                    viewIdFieldName = viewConfigurationNode.get(fieldName).textValue();
                }
                JsonNode got = viewConfigurationNode.get(fieldName);
                if (got != null && !got.isNull() && got.isObject() && got.has(ReservedFields.VIEW_OBJECT_ID)) {
                    viewClassName = fieldName;
                    viewObject = ViewObject.builder((ObjectNode) got).build();
                }
            }
            if (viewClassName == null) {
                throw new IllegalStateException("Failed to locate a valid viewObject definition.");
            }
            return new ViewModel(viewClassName, idCentric, notifiable, viewIdFieldName, viewObject);
        }
    }

    @Override
    public String toString() {
        return "ViewModel{"
            + "viewClassName=" + viewClassName
            + ", idCentric=" + idCentric
            + ", viewIdFieldName=" + viewIdFieldName
            + ", viewObject=" + viewObject + '}';
    }

    @Override
    public int hashCode() {
        int hash = 5;
        hash = 97 * hash + Objects.hashCode(this.viewClassName);
        hash = 97 * hash + (this.idCentric ? 1 : 0);
        hash = 97 * hash + Objects.hashCode(this.viewIdFieldName);
        hash = 97 * hash + Objects.hashCode(this.viewObject);
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
        final ViewModel other = (ViewModel) obj;
        if (!Objects.equals(this.viewClassName, other.viewClassName)) {
            return false;
        }
        if (this.idCentric != other.idCentric) {
            return false;
        }
        if (!Objects.equals(this.viewIdFieldName, other.viewIdFieldName)) {
            return false;
        }
        if (!Objects.equals(this.viewObject, other.viewObject)) {
            return false;
        }
        return true;
    }
}
