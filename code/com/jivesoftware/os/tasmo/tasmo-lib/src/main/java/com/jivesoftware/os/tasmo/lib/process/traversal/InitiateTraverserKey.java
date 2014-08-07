/*
 * $Revision$
 * $Date$
 *
 * Copyright (C) 1999-$year$ Jive Software. All rights reserved.
 *
 * This software is the proprietary information of Jive Software. Use is subject to license terms.
 */
package com.jivesoftware.os.tasmo.lib.process.traversal;

import com.jivesoftware.os.tasmo.event.api.ReservedFields;
import java.util.Objects;

public class InitiateTraverserKey {

    private final String triggerFieldName;
    private final String refFieldName;
    private final boolean centric;

    public InitiateTraverserKey(String triggerFieldName, String refFieldName, boolean centric) {
        this.triggerFieldName = triggerFieldName;
        this.refFieldName = refFieldName;
        this.centric = centric;
    }

    public String getTriggerFieldName() {
        return triggerFieldName;
    }

    public String getRefFieldName() {
        return refFieldName;
    }

    public boolean isDelete() {
        return ReservedFields.DELETED.equals(triggerFieldName);
    }

    public boolean isCentric() {
        return centric;
    }

    @Override
    public String toString() {
        return "InitiateTraverserKey{" + "triggerFieldName=" + triggerFieldName + ", refFieldName=" + refFieldName + ", centric=" + centric + '}';
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 53 * hash + Objects.hashCode(this.triggerFieldName);
        hash = 53 * hash + Objects.hashCode(this.refFieldName);
        hash = 53 * hash + (this.centric ? 1 : 0);
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
        final InitiateTraverserKey other = (InitiateTraverserKey) obj;
        if (!Objects.equals(this.triggerFieldName, other.triggerFieldName)) {
            return false;
        }
        if (!Objects.equals(this.refFieldName, other.refFieldName)) {
            return false;
        }
        if (this.centric != other.centric) {
            return false;
        }
        return true;
    }

}
