package com.jivesoftware.os.tasmo.event.api.write;

import com.jivesoftware.os.jive.utils.id.Id;
import com.jivesoftware.os.jive.utils.id.ObjectId;
import com.jivesoftware.os.jive.utils.id.TenantId;
import com.jivesoftware.os.jive.utils.ordered.id.ConstantWriterIdProvider;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProviderImpl;
import com.jivesoftware.os.tasmo.id.IdProviderImpl;
import org.testng.Assert;
import org.testng.annotations.Test;

public class EventBuilderTest {

    private final TenantId tenantId = new TenantId("tenantId");
    private final Id actorId = new Id(1);

    //test create and update jsonEvent
    @Test
    public void testJsonEventCreateUpdate() throws Exception {
        //create jsonevent
        IdProviderImpl idProvider = new IdProviderImpl(new OrderIdProviderImpl(new ConstantWriterIdProvider(100)));
        Event event = EventBuilder.create(idProvider, EventBuilderTest.class.getSimpleName(), tenantId, actorId).build();
        Assert.assertEquals(event.getObjectId().getClassName(), EventBuilderTest.class.getSimpleName(), "compare class name");

        //update existing jsonevent
        ObjectId objectId = new ObjectId(EventBuilderTest.class.getSimpleName(), new Id(100));
        Event event2 = EventBuilder.update(objectId, tenantId, actorId).build();
        Assert.assertEquals(event2.getObjectId().getClassName(), EventBuilderTest.class.getSimpleName(), "compare class name");

        //update new jsonevent
        ObjectId objectId3 = new ObjectId("myClass", new Id(100));
        Event event3 = EventBuilder.update(objectId3, tenantId, actorId).build();
        Assert.assertEquals(event3.getObjectId().getClassName(), "myClass", "compare class name");

    }

    @Test (dataProviderClass = JsonEventTestDataProvider.class, dataProvider = "createJsonData")
    public void testJsonEventSet(String key, Object value) throws Exception {

        //create jsonEvent
        IdProviderImpl idProvider = new IdProviderImpl(new OrderIdProviderImpl(new ConstantWriterIdProvider(100)));
        Event event = EventBuilder.create(idProvider, EventBuilderTest.class.getSimpleName(), tenantId, actorId)
                .set(key, value)
            .build();
        Assert.assertEquals(event.getObjectId().getClassName(), EventBuilderTest.class.getSimpleName(), "compare class name");

        //better to check the result: do that later when get time
        System.out.println("updated jsonEvent:" + event.toString());

    }

    @Test (dataProviderClass = JsonEventTestDataProvider.class, dataProvider = "createBadJsonData",
        expectedExceptions = RuntimeException.class)
    public void testJsonEventSetBadData(String key, Object value) throws Exception {
        //create jsonEvent
        IdProviderImpl idProvider = new IdProviderImpl(new OrderIdProviderImpl(new ConstantWriterIdProvider(100)));
        Event event = EventBuilder.create(idProvider, EventBuilderTest.class.getSimpleName(), tenantId, actorId)
                .set(key, value)
            .build();
    }

    @Test
    public void testJsonEventCommit() throws Exception {
        //create new jsonEvent
        ObjectId objectId = new ObjectId("userClass", new Id(1000));
        Event event = EventBuilder.update(objectId, tenantId, actorId).build();

        //you can either commit one event for this event
        ObjectId writeObjectId = event.getObjectId();

        Assert.assertEquals(writeObjectId.getClassName(), "userClass");
        Assert.assertEquals(writeObjectId.getId(), new Id(1000));

    }
}
