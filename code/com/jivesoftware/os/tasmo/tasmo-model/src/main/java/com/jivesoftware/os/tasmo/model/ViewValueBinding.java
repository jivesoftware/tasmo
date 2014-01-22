/*
 * $Revision$
 * $Date$
 *
 * Copyright (C) 1999-$year$ Jive Software. All rights reserved.
 *
 * This software is the proprietary information of Jive Software. Use is subject to license terms.
 */
package com.jivesoftware.os.tasmo.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.jivesoftware.os.tasmo.model.path.ModelPath;

/**
 *
 */
public class ViewValueBinding {

    private final String pathId;
    private final ModelPath modelPath;

    @JsonCreator
    public ViewValueBinding(
        @JsonProperty("pathId") String pathId,
        @JsonProperty("modelPath") ModelPath modelPath) {
        if (pathId == null || pathId.length() == 0) {
            throw new IllegalArgumentException("viewFieldName cannot be null or an empty string.");
        }
        this.pathId = pathId;
        this.modelPath = modelPath;
    }

    public String getPathId() {
        return pathId;
    }

    public ModelPath getModelPath() {
        return modelPath;
    }

    @Override
    public String toString() {
        return "ViewValueBinding{" + "viewFieldName=" + pathId + ", modelPath=" + modelPath + '}';
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 53 * hash + (this.pathId != null ? this.pathId.hashCode() : 0);
        hash = 53 * hash + (this.modelPath != null ? this.modelPath.hashCode() : 0);
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
        final ViewValueBinding other = (ViewValueBinding) obj;
        if (this.pathId != other.pathId && (this.pathId == null || !this.pathId.equals(other.pathId))) {
            return false;
        }
        if (this.modelPath != other.modelPath && (this.modelPath == null || !this.modelPath.equals(other.modelPath))) {
            return false;
        }
        return true;
    }

}
