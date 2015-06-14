/*
 * $Revision$
 * $Date$
 *
 * Copyright (C) 1999-$year$ Jive Software. All rights reserved.
 *
 * This software is the proprietary information of Jive Software. Use is subject to license terms.
 */
package com.jivesoftware.os.tasmo.model;

import com.jivesoftware.os.jive.utils.id.TenantId;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ViewsProcessorIdTest {

    @Test
    public void inputValidationTest() {

        TenantId tenantId = new TenantId("foo");
        String viewsProcessorId = "bar";

        ViewsProcessorId processorId = new ViewsProcessorId(tenantId, viewsProcessorId);
        Assert.assertEquals(tenantId, processorId.getTenantId());
        Assert.assertEquals(viewsProcessorId, processorId.getProcessorId());

        int hashCode = processorId.hashCode();
        System.out.println("hashCode:" + hashCode);
        Assert.assertEquals(hashCode, 7_324_176);

        String toString = processorId.toString();
        System.out.println("toString:" + toString);
        Assert.assertEquals(toString, "ViewsProcessorId{tenantId=foo, processorId=bar}");
    }
}
