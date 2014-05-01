/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.jivesoftware.os.tasmo.model.process;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProviderImpl;
import com.jivesoftware.os.jive.utils.row.column.value.store.marshall.api.TypeMarshaller;
import com.jivesoftware.os.tasmo.event.api.EventVerb;
import com.jivesoftware.os.tasmo.event.api.JsonEventConventions;
import com.jivesoftware.os.tasmo.id.Id;
import com.jivesoftware.os.tasmo.id.ObjectId;
import com.jivesoftware.os.tasmo.id.TenantId;
import java.io.IOException;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 *
 * @author pete
 */
public class JsonWrittenEventProviderTest {

    private ObjectMapper mapper = new ObjectMapper();
    JsonEventConventions eventHelper = new JsonEventConventions();
    OrderIdProvider idProvider = new OrderIdProviderImpl(36);

    @Test
    public void testLiteralFieldValueMarshalling() throws Exception {
        WrittenEventProvider<ObjectNode, JsonNode> provider = new JsonWrittenEventProvider();

        JsonNode node = mapper.convertValue("booya", JsonNode.class);
        TypeMarshaller<OpaqueFieldValue> marshaller = provider.getLiteralFieldValueMarshaller();

        OpaqueFieldValue literal = provider.convertFieldValue(node);
        Assert.assertEquals(marshaller.fromLexBytes(marshaller.toLexBytes(literal)), literal);
    }

    @Test
    public void testConvertEvent() {

        ObjectNode event = mapper.createObjectNode();
        eventHelper.setActivityVerb(event, EventVerb.CREATED);

        Id actor = new Id(idProvider.nextId());
        eventHelper.setActorId(event, actor);


        Id centric = new Id(idProvider.nextId());
        eventHelper.setUserId(event, centric);

        String tenant = "jive";
        eventHelper.setTenantId(event, new TenantId(tenant));

        String eventClass = "test";
        ObjectNode instance = eventHelper.setInstanceClassName(event, eventClass);

        Id instanceId = new Id(idProvider.nextId());
        eventHelper.setInstanceId(event, instanceId, eventClass);

        long snowflake = idProvider.nextId();
        eventHelper.setEventId(event, snowflake);

        eventHelper.setTrackEventProcessedLifecycle(event, Boolean.TRUE);


        String value = "blah";
        instance.put("value", value);

        ObjectId ref = new ObjectId("Foo", new Id(idProvider.nextId()));
        instance.put("ref", ref.toStringForm());

        ArrayNode refs = mapper.createArrayNode();
        ObjectId refs1 = new ObjectId("Bar", new Id(idProvider.nextId()));
        ObjectId refs2 = new ObjectId("Bar", new Id(idProvider.nextId()));

        refs.add(refs1.toStringForm());
        refs.add(refs2.toStringForm());

        instance.put("refs", refs);


        WrittenEventProvider<ObjectNode, JsonNode> provider = new JsonWrittenEventProvider();
        WrittenEvent writtenEvent = provider.convertEvent(event);

        Assert.assertEquals(writtenEvent.getActorId(), actor);
        Assert.assertEquals(writtenEvent.getCentricId(), centric);
        Assert.assertEquals(writtenEvent.getEventId(), snowflake);
        Assert.assertEquals(writtenEvent.getTenantId(), new TenantId(tenant));

        WrittenInstance payload = writtenEvent.getWrittenInstance();
        Assert.assertEquals(payload.getInstanceId(), new ObjectId(eventClass, instanceId));

        Assert.assertTrue(payload.hasField("value"));
        Assert.assertTrue(payload.hasField("ref"));
        Assert.assertTrue(payload.hasField("refs"));

        JsonNode valueAsJson = mapper.convertValue(value, JsonNode.class);
        JsonNode refAsJson = mapper.convertValue(ref.toStringForm(), JsonNode.class);
        Assert.assertEquals(payload.getFieldValue("value"), provider.convertFieldValue(valueAsJson));
        Assert.assertEquals(payload.getFieldValue("ref"), provider.convertFieldValue(refAsJson));
        Assert.assertEquals(payload.getFieldValue("refs"), provider.convertFieldValue(refs));
    }


    @Test
    public void testStringifyValueMap() throws IOException {
        WrittenEventProvider<ObjectNode, JsonNode> provider = new JsonWrittenEventProvider();
        ObjectNode object = mapper.createObjectNode();

        object.put("value1", "booya");

        ArrayNode array = mapper.createArrayNode();
        array.add(true);
        array.add(false);
        object.put("value2", array);

        OpaqueFieldValue value1 = provider.convertFieldValue(mapper.convertValue("booya", JsonNode.class));
        OpaqueFieldValue value2 = provider.convertFieldValue(array);

        LeafNodeFields values = provider.createLeafNodeFields();

        values.addField("value1", value1);
        values.addField("value2", value2);

        byte[] stringified = values.toBytes();
        Assert.assertEquals(stringified, mapper.writeValueAsBytes(object));
    }
}
