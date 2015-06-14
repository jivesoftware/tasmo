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

    value(true, false, false),
    ref(false, false, false),
    latest_backRef(false, true, false),
    backRefs(false, true, false),
    count(false, true, false),
    refs(false, false, false),
    centric_value(true, false, true),
    centric_ref(false, false, true),
    centric_latest_backRef(false, true, true),
    centric_backRefs(false, true, true),
    centric_count(false, true, true),
    centric_refs(false, false, true);

    private final boolean valueType;
    private final boolean backReferenceType;
    private final boolean centric;

    private ModelPathStepType(boolean valueType, boolean backReferenceType, boolean centric) {
        this.valueType = valueType;
        this.backReferenceType = backReferenceType;
        this.centric = centric;
    }

    public boolean isBackReferenceType() {
        return backReferenceType;
    }

    public boolean isValue() {
        return valueType;
    }

    public boolean isCentric() {
        return centric;
    }
}
