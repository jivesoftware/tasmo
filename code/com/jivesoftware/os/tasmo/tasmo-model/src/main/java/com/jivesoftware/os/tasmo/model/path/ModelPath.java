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
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * (Class.Field.[BackRef, Ref, BackRefs, Refs, Value]) ->
 */
public class ModelPath {

    private final String id;
    private final List<ModelPathStep> pathMembers;
    private int rootIdIndex;

    @JsonCreator
    public ModelPath(
        @JsonProperty("id") String id,
        @JsonProperty("pathMembers") List<ModelPathStep> pathMembers) {
        this.id = id;
        this.pathMembers = Collections.unmodifiableList(pathMembers);

        boolean assignedRootId = false;
        for (int i = 0; i < this.pathMembers.size(); i++) {
            ModelPathStep member = pathMembers.get(i);
            if (member.getIsRootId()) {
                if (assignedRootId) {
                    throw new RuntimeException("should only have one root id declared, check " + member.toString());
                } else {
                    this.rootIdIndex = i;
                    assignedRootId = true;
                }
            }
        }
        if (!assignedRootId) {
            throw new IllegalStateException("ModelPath must have a root id.");
        }
    }

    public String getId() {
        return id;
    }

    public List<ModelPathStep> getPathMembers() {
        return pathMembers;
    }

    @JsonIgnore
    public Set<String> getRootClassNames() {
        for (ModelPathStep member : pathMembers) {
            if (member.getIsRootId()) {
                if (member.getStepType().isBackReferenceType()) {
                    return member.getDestinationClassNames();
                }
                return member.getOriginClassNames();
            }
        }
        throw new IllegalStateException("This should be impossible!");
    }

    @JsonIgnore
    public long getRootOrderId(long[] orderIds) {
        return orderIds[rootIdIndex];
    }

    @JsonIgnore
    public String pathToAsString() {
        return Joiner.on("|").join(pathMembers);
    }

    @JsonIgnore
    public int getPathMemberSize() {
        return pathMembers.size();
    }

    @Override
    public String toString() {
        return "ModelPath{" + "pathMembers=" + pathMembers + '}';
    }

    @Override
    public int hashCode() {
        int hash = 3;
        hash = 23 * hash + (this.id != null ? this.id.hashCode() : 0);
        hash = 23 * hash + (this.pathMembers != null ? this.pathMembers.hashCode() : 0);
        hash = 23 * hash + this.rootIdIndex;
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
        final ModelPath other = (ModelPath) obj;
        if ((this.id == null) ? (other.id != null) : !this.id.equals(other.id)) {
            return false;
        }
        if (this.pathMembers != other.pathMembers && (this.pathMembers == null || !this.pathMembers.equals(other.pathMembers))) {
            return false;
        }
        if (this.rootIdIndex != other.rootIdIndex) {
            return false;
        }
        return true;
    }

    public static Builder builder(String id) {
        return new Builder(id);
    }

    public static class Builder {

        private final List<ModelPathStep> pathMembers = Lists.newLinkedList();
        private final String id;

        private Builder(String id) {
            this.id = id;
        }

        public Builder addPathMember(ModelPathStep pathMember) {
            pathMembers.add(pathMember);
            return this;
        }

        public Builder addPathMembers(List<ModelPathStep> pathMembers) {
            for (ModelPathStep pathMember : pathMembers) {
                addPathMember(pathMember);
            }

            return this;
        }

        public ModelPath build() {
            if (pathMembers.isEmpty()) {
                throw new IllegalStateException("A path must consist of at least one member");
            }

            for (int i = 0; i < pathMembers.size(); i++) {
                ModelPathStep step = pathMembers.get(i);
                if (i < pathMembers.size() - 1
                        && (step.getStepType().equals(ModelPathStepType.value)
                        || (step.getFieldNames() != null && !step.getFieldNames().isEmpty()))) {
                    throw new IllegalArgumentException("Only leaf nodes of a model path can be value type steps");
                }
            }
            return new ModelPath(id, pathMembers);
        }
    }
}
