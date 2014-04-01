package com.jivesoftware.os.tasmo.local;

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