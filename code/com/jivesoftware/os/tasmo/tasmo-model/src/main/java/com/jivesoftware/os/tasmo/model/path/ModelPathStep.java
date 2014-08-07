/*
 * $Revision$
 * $Date$
 *
 * Copyright (C) 1999-$year$ Jive Software. All rights reserved.
 *
 * This software is the proprietary information of Jive Software. Use is subject to license terms.
 */
package com.jivesoftware.os.tasmo.model.path;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Joiner;
import com.google.common.collect.Sets;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

/**
 *
 */
public class ModelPathStep {

    private final boolean isRootId;
    private final Set<String> originClassName;
    // refFieldName
    private final String refFieldName;
    private final ModelPathStepType stepType;
    private final Set<String> destinationClassName;
    //leafFieldNames
    private final List<String> fieldNames;

    // only public for jackson
    @JsonCreator
    public ModelPathStep(@JsonProperty (value = "isRootId")
        boolean isRootId,
        @JsonProperty (value = "originClassName")
        Set<String> originClassName,
        @JsonProperty (value = "refFieldName")
        String refFieldName,
        @JsonProperty (value = "stepType")
        ModelPathStepType stepType,
        @JsonProperty (value = "destinationClassName")
        Set<String> destinationClassName,
        @JsonProperty (value = "fieldNames")
        List<String> fieldNames) {

        this.isRootId = isRootId;
        this.originClassName = originClassName;
        this.stepType = stepType;
        if (stepType == ModelPathStepType.value) {
            if (refFieldName != null) {
                throw new IllegalArgumentException("ModelPathStep 'refFieldName' must be null for stepType:" + stepType);
            }
            this.refFieldName = refFieldName;
            if (fieldNames == null || fieldNames.isEmpty()) {
                throw new IllegalArgumentException("ModelPathStep 'fieldNames' must be non-null with 1 or more values for stepType:" + stepType);
            }
            this.fieldNames = fieldNames;
            if (destinationClassName != null) {
                throw new IllegalArgumentException("ModelPathStep 'destinationClassName' must be null for stepType:" + stepType);
            }
            this.destinationClassName = destinationClassName;

        } else {
            if (refFieldName == null) {
                throw new IllegalArgumentException("ModelPathStep 'refFieldName' cannot be null for stepType:" + stepType);
            }
            this.refFieldName = refFieldName;
            if (fieldNames != null && !fieldNames.isEmpty()) {
                throw new IllegalArgumentException("ModelPathStep 'fieldNames' must be null or empty  for stepType:" + stepType);
            }
            this.fieldNames = fieldNames;
            if (destinationClassName == null) {
                throw new IllegalArgumentException("ModelPathStep 'destinationClassName' must be non-null for stepType:" + stepType);
            }
            this.destinationClassName = destinationClassName;
        }
    }

    public ModelPathStep(boolean isRootId,
        String originClassName, String refFieldName, ModelPathStepType stepType, String destinationClassName) {
        this(isRootId, Sets.newHashSet(originClassName), refFieldName, stepType, Sets.newHashSet(destinationClassName), null);
    }

    public ModelPathStep(boolean isRootId, String originClassName, List<String> valueFields, boolean centric) {
        this(isRootId, Sets.newHashSet(originClassName), null, ModelPathStepType.value, null, valueFields);
    }

    public boolean getIsRootId() {
        return isRootId;
    }

    public Set<String> getOriginClassNames() {
        return originClassName;
    }

    public String getRefFieldName() {
        return refFieldName;
    }

    public ModelPathStepType getStepType() {
        return stepType;
    }

    public Set<String> getDestinationClassNames() {
        return destinationClassName;
    }

    public List<String> getFieldNames() {
        if (fieldNames == null) {
            return Collections.emptyList();
        } else {
            return fieldNames;
        }
    }

    @Override
    public String toString() {
        List<Object> path = new LinkedList<>();
        switch (stepType) {
            case centric_value:
                path.add("centric");
            case value:
                path.add(originClassName);
                path.add(isRootId ? "pid" : "id");
                path.add(Joiner.on(",").join(fieldNames));
                break;

            case centric_ref:
            case centric_refs:
                path.add("centric");
            case ref:
            case refs:
                path.add(originClassName);
                path.add(isRootId ? "pid" : "id");
                path.add(refFieldName);
                path.add(stepType);
                path.add(destinationClassName);
                break;

            case centric_latest_backRef:
            case centric_backRefs:
            case centric_count:
                path.add("centric");
            case latest_backRef:
            case backRefs:
            case count:
                path.add(destinationClassName);
                path.add(stepType);
                path.add(originClassName);
                path.add(isRootId ? "pid" : "id");
                path.add(refFieldName);
                break;
        }
        return Joiner.on(".").join(path);
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 11 * hash + (this.isRootId ? 1 : 0);
        hash = 11 * hash + (this.originClassName != null ? this.originClassName.hashCode() : 0);
        hash = 11 * hash + (this.refFieldName != null ? this.refFieldName.hashCode() : 0);
        hash = 11 * hash + (this.stepType != null ? this.stepType.hashCode() : 0);
        hash = 11 * hash + (this.destinationClassName != null ? this.destinationClassName.hashCode() : 0);
        hash = 11 * hash + (this.fieldNames != null ? this.fieldNames.hashCode() : 0);
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
        final ModelPathStep other = (ModelPathStep) obj;
        if (this.isRootId != other.isRootId) {
            return false;
        }
        if (this.originClassName != other.originClassName && (this.originClassName == null || !this.originClassName.equals(other.originClassName))) {
            return false;
        }
        if (this.refFieldName != other.refFieldName && (this.refFieldName == null || !this.refFieldName.equals(other.refFieldName))) {
            return false;
        }
        if (this.stepType != other.stepType) {
            return false;
        }
        if (this.destinationClassName != other.destinationClassName
            && (this.destinationClassName == null || !this.destinationClassName.equals(other.destinationClassName))) {
            return false;
        }
        if (this.fieldNames != other.fieldNames && (this.fieldNames == null || !this.fieldNames.equals(other.fieldNames))) {
            return false;
        }
        return true;
    }
}
