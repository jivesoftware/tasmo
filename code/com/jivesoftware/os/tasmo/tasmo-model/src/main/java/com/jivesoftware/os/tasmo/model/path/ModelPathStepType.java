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

    value(false), ref(false), latest_backRef(true), backRefs(true), count(true), refs(false);
    private final boolean backReferenceType;

    private ModelPathStepType(boolean backReferenceType) {
        this.backReferenceType = backReferenceType;
    }

    public boolean isBackReferenceType() {
        return backReferenceType;
    }
}
