/*
 * $Revision$
 * $Date$
 *
 * Copyright (C) 1999-$year$ Jive Software. All rights reserved.
 *
 * This software is the proprietary information of Jive Software. Use is subject to license terms.
 */
package com.jivesoftware.os.tasmo.model.path;

/**
 *
 */
public enum ModelPathStepType {

    value(false, false), ref(false, false), latest_backRef(true, false), backRefs(true, false), count(true, false), refs(false, false),
    centric_value(false, true), centric_ref(false, true), centric_latest_backRef(true, true), centric_backRefs(true, true),
    centric_count(true, true), centric_refs(false, true);

    private final boolean backReferenceType;
    private final boolean centric;

    private ModelPathStepType(boolean backReferenceType, boolean centric) {
        this.backReferenceType = backReferenceType;
        this.centric = centric;
    }

    public boolean isBackReferenceType() {
        return backReferenceType;
    }

    public boolean isCentric() {
        return centric;
    }

}
