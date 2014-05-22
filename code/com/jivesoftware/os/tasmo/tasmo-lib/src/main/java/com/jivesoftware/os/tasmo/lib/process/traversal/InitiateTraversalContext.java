/*
 * $Revision$
 * $Date$
 *
 * Copyright (C) 1999-$year$ Jive Software. All rights reserved.
 *
 * This software is the proprietary information of Jive Software. Use is subject to license terms.
 */
package com.jivesoftware.os.tasmo.lib.process.traversal;

import com.google.common.collect.Sets;
import com.jivesoftware.os.tasmo.event.api.ReservedFields;
import com.jivesoftware.os.tasmo.model.path.ModelPathStep;
import com.jivesoftware.os.tasmo.model.path.ModelPathStepType;
import java.util.List;
import java.util.Set;

/**
 *
 */
public class InitiateTraversalContext {

    private final ModelPathStep initialModelPathMember;
    private final int pathIndex;
    private final int membersSize;
    private final String viewClassName;
    private final String modelPathId;
    private final Set<String> allInitialFieldNames = Sets.newHashSet();

    public InitiateTraversalContext(
        ModelPathStep initialModelPathMember,
        int pathIndex,
        int membersSize,
        String viewclassName,
        String modelPathId) {
        this.initialModelPathMember = initialModelPathMember;
        this.pathIndex = pathIndex;
        this.membersSize = membersSize;
        this.viewClassName = viewclassName;
        this.modelPathId = modelPathId;

        List<String> fieldNames = initialModelPathMember.getFieldNames();
        if (fieldNames != null && !fieldNames.isEmpty()) {
            allInitialFieldNames.addAll(fieldNames);
        }
        allInitialFieldNames.add(ReservedFields.DELETED);
    }

    public Set<String> getInitialClassNames() {
        return initialModelPathMember.getOriginClassNames();
    }

    public ModelPathStep getInitialModelPathStep() {
        return initialModelPathMember;
    }

    public ModelPathStepType getInitialModelPathStepType() {
        return initialModelPathMember.getStepType();
    }

    public int getPathIndex() {
        return pathIndex;
    }

    public String getRefFieldName() {
        return initialModelPathMember.getRefFieldName();
    }

    public Set<String> getInitialFieldNames() {
        return allInitialFieldNames;
    }

    public int getMembersSize() {
        return membersSize;
    }

    @Override
    public String toString() {
        return "InitialStep{ viewClassName=" + viewClassName
            + ", initialModelPathMember=" + initialModelPathMember
            + ", pathIndex=" + pathIndex
            + ", membersSize=" + membersSize
            + ", modelPathId=" + modelPathId
            + '}';
    }
}
