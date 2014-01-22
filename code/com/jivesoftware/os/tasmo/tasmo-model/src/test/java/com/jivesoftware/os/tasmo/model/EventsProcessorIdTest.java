/*
 * $Revision$
 * $Date$
 *
 * Copyright (C) 1999-$year$ Jive Software. All rights reserved.
 *
 * This software is the proprietary information of Jive Software. Use is subject to license terms.
 */
package com.jivesoftware.os.tasmo.model;

import com.jivesoftware.os.tasmo.id.TenantId;
import org.testng.Assert;
import org.testng.annotations.Test;

public class EventsProcessorIdTest {

    @Test
    public void inputValidationTest() {

        TenantId tenantId = new TenantId("foo");
        String viewsProcessorId = "bar";

        EventsProcessorId processorId = new EventsProcessorId(tenantId, viewsProcessorId);
        Assert.assertEquals(tenantId, processorId.getTenantId());
        Assert.assertEquals(viewsProcessorId, processorId.getProcessorId());

        int hashCode = processorId.hashCode();
        System.out.println("hashCode:" + hashCode);
        Assert.assertEquals(hashCode, 7324176);

        String toString = processorId.toString();
        System.out.println("toString:" + toString);
        Assert.assertEquals(toString, "EventsProcessorId{tenantId=foo, processorId=bar}");
    }
}
