/*
 * $Revision$
 * $Date$
 *
 * Copyright (C) 1999-$year$ Jive Software. All rights reserved.
 *
 * This software is the proprietary information of Jive Software. Use is subject to license terms.
 */
package com.jivesoftware.os.tasmo.lib.process;

import com.jivesoftware.os.tasmo.event.api.ReservedFields;

public class InitialStepKey {

    private final String triggerFieldName;
    private final String refFieldName;

    public InitialStepKey(String triggerFieldName, String refFieldName) {
        this.triggerFieldName = triggerFieldName;
        this.refFieldName = refFieldName;
    }

    public String getTriggerFieldName() {
        return triggerFieldName;
    }

    public String getInitialFieldName() {
        return refFieldName;
    }

    public boolean isDelete() {
        return ReservedFields.DELETED.equals(triggerFieldName);
    }

    @Override
    public String toString() {
        return "InitialStepKey{" + "triggerFieldName=" + triggerFieldName + ", refFieldName=" + refFieldName + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        InitialStepKey that = (InitialStepKey) o;

        if (refFieldName != null ? !refFieldName.equals(that.refFieldName) : that.refFieldName != null) {
            return false;
        }
        if (triggerFieldName != null ? !triggerFieldName.equals(that.triggerFieldName) : that.triggerFieldName != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = triggerFieldName != null ? triggerFieldName.hashCode() : 0;
        result = 31 * result + (refFieldName != null ? refFieldName.hashCode() : 0);
        return result;
    }
}
