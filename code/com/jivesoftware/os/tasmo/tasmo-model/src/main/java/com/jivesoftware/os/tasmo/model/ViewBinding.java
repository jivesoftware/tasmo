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
import com.google.common.collect.Sets;
import com.jivesoftware.os.tasmo.event.api.ReservedFields;
import com.jivesoftware.os.tasmo.model.path.ModelPath;
import com.jivesoftware.os.tasmo.model.path.ModelPathStep;
import com.jivesoftware.os.tasmo.model.path.ModelPathStepType;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 *
 */
public class ViewBinding {

    private final String viewClassName;
    private final List<ModelPath> modelPaths;
    private final boolean persistChanges; // deprecated but left here since it has been deserialized and persisted in hbase
    private final boolean idCentric;
    private final boolean notifiable;
    private final String viewIdFieldName;

    @JsonCreator
    public ViewBinding(
        @JsonProperty("viewClassName") String viewClassName,
        @JsonProperty("modelPaths") List<ModelPath> modelPaths,
        @JsonProperty("persistChanges") boolean persistChanges,
        @JsonProperty("idCentric") boolean idCentric,
        @JsonProperty("notifiable") boolean notifiable,
        @JsonProperty("viewIdFieldName") String viewIdFieldName) {
        if (viewClassName == null || viewClassName.length() == 0) {
            throw new IllegalArgumentException("viewClassName cannot be null or an empty string.");
        }
        this.viewClassName = viewClassName;
        this.modelPaths = modelPaths;
        this.persistChanges = persistChanges;
        this.idCentric = idCentric;
        this.notifiable = notifiable;
        this.viewIdFieldName = viewIdFieldName;
        validate();
    }

    private void validate() {
        Set<String> ensurePathIdsAreUnique = new HashSet<>();
        Set<Integer> ensurePathIdHashCodesAreUnique = new HashSet<>();
        Set<String> rootClassNames = new HashSet<>();

        for (ModelPath modelPath : modelPaths) {
            if (modelPath == null) {
                throw new IllegalStateException("null viewValueBindings are not supported.");
            }
            String pathId = modelPath.getId();

            if (ensurePathIdsAreUnique.contains(pathId)) {
                throw new IllegalStateException("There are two occurances of ViewValueBinding using the same pathId:" + pathId + ".");
            }
            ensurePathIdsAreUnique.add(pathId);

            if (ensurePathIdHashCodesAreUnique.contains(pathId.hashCode())) {
                throw new IllegalStateException("The following pathId:" + pathId
                    + " .hashCode() collides with another pathId. Consider yourself lucky this check is here. This is incredible rare.");
            }
            ensurePathIdHashCodesAreUnique.add(pathId.hashCode());

            List<ModelPathStep> members = modelPath.getPathMembers();
            ModelPathStep leafStep = members.get(members.size() - 1);
            if (leafStep.getFieldNames().contains(ReservedFields.DELETED)) {
                throw new IllegalStateException("Model path " + pathId + " attempts to bind to the '" + ReservedFields.DELETED + "' field");
            }

            if (!leafStep.getStepType().equals(ModelPathStepType.value)) {
                throw new IllegalStateException("Model path " + pathId + " does not end with a value step");
            }

            Set<String> pathRootClasses = modelPath.getRootClassNames();

            if (rootClassNames.isEmpty()) {
                rootClassNames.addAll(pathRootClasses);
            } else if (Sets.difference(rootClassNames, pathRootClasses).size() > 0
                || Sets.difference(pathRootClasses, rootClassNames).size() > 0) {
                throw new IllegalStateException("all paths must pivot on the same root classNames:" + rootClassNames + " " + this);
            }
        }
    }

    public String getViewClassName() {
        return viewClassName;
    }

    public List<ModelPath> getModelPaths() {
        return modelPaths;
    }

    /**
     * @deprecated This used to be used by the view producer, but View notifications replaced this. It's currently left here because this class is serialized
     * and persisted in hbase and removing it would break deserialization.
     * @return true if changes are to be persisted.
     */
    @Deprecated
    public boolean isPersistChanges() {
        return persistChanges;
    }

    public boolean isIdCentric() {
        return idCentric;
    }

    /**
     * Get if the view requires notification on changes.<br/>
     * Currently hard-coded, making configurable is in the works.
     */
    public boolean isNotificationRequired() {
        return notifiable;
    }

    public String getViewIdFieldName() {
        return viewIdFieldName;
    }

    @Override
    public String toString() {
        return "ViewValueBindings{" + "viewClassName=" + viewClassName + ", viewValueBindings=" + modelPaths + '}';
    }

    @Override
    public int hashCode() {
        int hash = 3;
        hash = 47 * hash + (this.viewClassName != null ? this.viewClassName.hashCode() : 0);
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
        final ViewBinding other = (ViewBinding) obj;
        if ((this.viewClassName == null) ? (other.viewClassName != null) : !this.viewClassName.equals(other.viewClassName)) {
            return false;
        }
        return true;
    }
}
