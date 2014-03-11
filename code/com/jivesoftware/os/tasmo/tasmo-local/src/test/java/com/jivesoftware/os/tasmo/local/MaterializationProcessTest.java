package com.jivesoftware.os.tasmo.local;

import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProviderImpl;
import com.jivesoftware.os.tasmo.id.Id;
import com.jivesoftware.os.tasmo.id.IdProvider;
import com.jivesoftware.os.tasmo.id.IdProviderImpl;
import com.jivesoftware.os.tasmo.id.TenantId;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 *
 */

public class MaterializationProcessTest {
/*
    @Test
    public void testWriteEventsAndReadView() throws Exception {

        LocalMaterializationSystem localMaterializationSystem =
            new LocalMaterializationSystemBuilder().setUseHBase(false).setFilterToTheseViewClasses(PlatformMonitorView.class).build();

        MaterializationProcess materializationProcess = new MaterializationProcess(localMaterializationSystem);

        IdProvider idProvider = new IdProviderImpl(new OrderIdProviderImpl(345));

        TenantId tenantId = new TenantId("booya");
        Id actor = new Id(6457);

        PlatformMonitorEventBuilder platformMonitorEventBuilder = new PlatformMonitorEventBuilder(idProvider,
            tenantId, actor);

        long lastTraceValue = 12345;

        platformMonitorEventBuilder.setLastTraceId(lastTraceValue);

        PlatformMonitorView platformMonitorView =
            materializationProcess.writeEventAndReadView(platformMonitorEventBuilder.build(), PlatformMonitorView.class);

        Assert.assertNotNull(platformMonitorView);
        Assert.assertEquals(platformMonitorView.lastTraceId(), lastTraceValue);
    }
     */
}