package com.jivesoftware.os.tasmo.model.process;

import com.jivesoftware.os.jive.utils.id.ObjectId;
import com.jivesoftware.os.jive.utils.id.TenantIdAndCentricId;

public class ModifiedViewInfo {

    private final TenantIdAndCentricId tenantIdAndCentricId;
    private final ObjectId viewId;

    public ModifiedViewInfo(TenantIdAndCentricId tenantIdAndCentricId, ObjectId viewId) {
        this.tenantIdAndCentricId = tenantIdAndCentricId;
        this.viewId = viewId;
    }

    public ObjectId getViewId() {
        return viewId;
    }

    public TenantIdAndCentricId getTenantIdAndCentricId() {
        return tenantIdAndCentricId;
    }

    @Override
    public String toString() {
        return "ModifiedViewInfo{" + "tenantIdAndCentricId=" + tenantIdAndCentricId + ", viewId=" + viewId + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ModifiedViewInfo that = (ModifiedViewInfo) o;

        if (!tenantIdAndCentricId.equals(that.tenantIdAndCentricId)) {
            return false;
        }
        if (!viewId.equals(that.viewId)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = tenantIdAndCentricId.hashCode();
        result = 31 * result + viewId.hashCode();
        return result;
    }
}
