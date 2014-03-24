/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.jivesoftware.os.tasmo.configuration.events;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.jivesoftware.os.tasmo.event.api.JsonEventConventions;
import com.jivesoftware.os.tasmo.id.ChainedVersion;
import com.jivesoftware.os.tasmo.id.Id;
import com.jivesoftware.os.tasmo.id.ObjectId;
import com.jivesoftware.os.tasmo.id.TenantId;
import com.jivesoftware.os.tasmo.model.EventDefinition;
import com.jivesoftware.os.tasmo.model.EventsProcessorId;
import com.jivesoftware.os.tasmo.model.EventsProvider;
import com.jivesoftware.os.tasmo.model.TenantEventsProvider;
import com.jivesoftware.os.tasmo.model.VersionedEventsModel;
import java.io.IOException;
import java.util.Arrays;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class EventValidatorTest {

    private ObjectMapper mapper = new ObjectMapper();
    private EventsProvider eventsProvider;
    private TenantEventsProvider tenantEventsProvider;
    private EventValidator eventValidator;
    private TenantId tenantId = new TenantId("master");
    private ChainedVersion version = new ChainedVersion("", "1");
    private EventDefinition event;
    private JsonEventConventions jec = new JsonEventConventions();

    @BeforeMethod
    public void setUpMethod() throws Exception {
        eventsProvider = Mockito.mock(EventsProvider.class);
        tenantEventsProvider = new TenantEventsProvider(tenantId, eventsProvider);
        eventValidator = new EventValidator(true);

        ObjectNode instance = mapper.createObjectNode();
        instance.put("value", "value");
        instance.put("ref_field", mapper.convertValue(new ObjectId("Bar", new Id(2)).toStringForm(), JsonNode.class));
        instance.put("refs_field", mapper.convertValue(Arrays.asList(new ObjectId("Baz", new Id(3)).toStringForm()), JsonNode.class));
        instance.put("all_field", mapper.convertValue(Arrays.asList(new ObjectId("Goo", new Id(4)).toStringForm()), JsonNode.class));

        event = toEvent("Foo", instance);
    }

    @Test
    public void testValidateEvent() throws IOException {

        ObjectNode instance = mapper.createObjectNode();
        instance.put("value", "value");
        instance.put("ref_field", mapper.convertValue(new ObjectId("Bar", new Id(2)).toStringForm(), JsonNode.class));
        instance.put("refs_field", mapper.convertValue(Arrays.asList(new ObjectId("Baz", new Id(3)).toStringForm()), JsonNode.class));
        instance.put("all_field", mapper.convertValue(Arrays.asList(new ObjectId("Goo", new Id(4)).toStringForm()), JsonNode.class));

        ObjectNode firedEvent = mapper.createObjectNode();
        jec.setEventId(firedEvent, 1);
        jec.setInstanceNode(firedEvent, "Bar", instance);

        EventDefinition definition2 = toEvent("Bar", instance);

        ChainedVersion version2 = new ChainedVersion("1", "2");
        Mockito.when(eventsProvider.getCurrentEventsVersion(tenantId))
            .thenReturn(version, version2);
        Mockito.when(eventsProvider.getEvents(Mockito.any(EventsProcessorId.class)))
            .thenReturn(Arrays.asList(event), Arrays.asList(definition2));

        VersionedEventsModel versionedEventsModel = tenantEventsProvider.getVersionedEventsModel(tenantId);
        Validated validateEvent = eventValidator.validateEvent(versionedEventsModel, firedEvent);
        Assert.assertFalse(validateEvent.isValid());

        tenantEventsProvider.loadModel(tenantId);
        versionedEventsModel = tenantEventsProvider.getVersionedEventsModel(tenantId);
        validateEvent = eventValidator.validateEvent(versionedEventsModel, firedEvent);
        Assert.assertTrue(validateEvent.isValid());

    }

    @Test
    public void testValidateMulitRefsEvent() {

        Mockito.when(eventsProvider.getCurrentEventsVersion(tenantId))
            .thenReturn(version, version);
        Mockito.when(eventsProvider.getEvents(Mockito.any(EventsProcessorId.class)))
            .thenReturn(Arrays.asList(event));

        ObjectNode instance = mapper.createObjectNode();
        instance.put("value", "value");
        instance.put("ref_field", mapper.convertValue(new ObjectId("Bar", new Id(2)).toStringForm(), JsonNode.class));
        instance.put("refs_field", mapper.convertValue(
            Arrays.asList(new ObjectId("Baz", new Id(3)).toStringForm(),
            new ObjectId("Gaga", new Id(7)).toStringForm()), JsonNode.class));
        instance.put("all_field", mapper.convertValue(
            Arrays.asList(new ObjectId("Goo", new Id(4)).toStringForm(),
            new ObjectId("Gaga", new Id(7)).toStringForm()), JsonNode.class));

        ObjectNode event2 = mapper.createObjectNode();
        jec.setEventId(event2, 1);
        jec.setInstanceNode(event2, "Foo", instance);

        tenantEventsProvider.loadModel(tenantId);
        VersionedEventsModel versionedEventsModel = tenantEventsProvider.getVersionedEventsModel(tenantId);
        Validated validateEvent = eventValidator.validateEvent(versionedEventsModel, event2);
        Assert.assertTrue(validateEvent.isValid());

    }

    @Test
    public void testInvalidEventClassEvent() {

        Mockito.when(eventsProvider.getCurrentEventsVersion(tenantId))
            .thenReturn(version, version);
        Mockito.when(eventsProvider.getEvents(Mockito.any(EventsProcessorId.class)))
            .thenReturn(Arrays.asList(event));

        ObjectNode instance = mapper.createObjectNode();
        instance.put("value", "value");

        ObjectNode event2 = mapper.createObjectNode();
        jec.setEventId(event2, 1);
        jec.setInstanceNode(event2, "Wrong", instance);

        tenantEventsProvider.loadModel(tenantId);
        VersionedEventsModel versionedEventsModel = tenantEventsProvider.getVersionedEventsModel(tenantId);
        Validated validateEvent = eventValidator.validateEvent(versionedEventsModel, event2);
        Assert.assertFalse(validateEvent.isValid());

    }

    @Test
    public void testInvalidFieldEvent() {
        Mockito.when(eventsProvider.getCurrentEventsVersion(tenantId))
            .thenReturn(version);
        Mockito.when(eventsProvider.getEvents(Mockito.any(EventsProcessorId.class)))
            .thenReturn(Arrays.asList(event));

        ObjectNode instance = mapper.createObjectNode();
        instance.put("value", "value");

        ObjectNode event2 = mapper.createObjectNode();
        jec.setEventId(event2, 1);
        jec.setInstanceNode(event2, "Foo", instance);

        tenantEventsProvider.loadModel(tenantId);
        VersionedEventsModel versionedEventsModel = tenantEventsProvider.getVersionedEventsModel(tenantId);
        Validated validateEvent = eventValidator.validateEvent(versionedEventsModel, event2);
        Assert.assertTrue(validateEvent.isValid());

        instance.put("wrong", "value");
        versionedEventsModel = tenantEventsProvider.getVersionedEventsModel(tenantId);
        validateEvent = eventValidator.validateEvent(versionedEventsModel, event2);
        Assert.assertFalse(validateEvent.isValid());

    }

    @Test
    public void testInvalidFieldTypeEvent() {

        Mockito.when(eventsProvider.getCurrentEventsVersion(tenantId))
            .thenReturn(version, version);
        Mockito.when(eventsProvider.getEvents(Mockito.any(EventsProcessorId.class)))
            .thenReturn(Arrays.asList(event));

        ObjectNode instance = mapper.createObjectNode();
        instance.put("value", "value");

        ObjectNode event2 = mapper.createObjectNode();
        jec.setEventId(event2, 1);
        jec.setInstanceNode(event2, "Foo", instance);

        tenantEventsProvider.loadModel(tenantId);
        VersionedEventsModel versionedEventsModel = tenantEventsProvider.getVersionedEventsModel(tenantId);
        Validated validateEvent = eventValidator.validateEvent(versionedEventsModel, event2);
        Assert.assertTrue(validateEvent.isValid());

        instance.put("value", mapper.convertValue(new ObjectId("Bar", new Id(2)).toStringForm(), JsonNode.class));
        versionedEventsModel = tenantEventsProvider.getVersionedEventsModel(tenantId);
        validateEvent = eventValidator.validateEvent(versionedEventsModel, event2);
        Assert.assertFalse(validateEvent.isValid());

        instance.put("value", mapper.convertValue(Arrays.asList(new ObjectId("Bar", new Id(2)).toStringForm()), JsonNode.class));
        versionedEventsModel = tenantEventsProvider.getVersionedEventsModel(tenantId);
        validateEvent = eventValidator.validateEvent(versionedEventsModel, event2);
        Assert.assertFalse(validateEvent.isValid());

    }

    @Test(expectedExceptions = RuntimeException.class)
    public void testThatItThrowAnExceptionIfYouHaventCalledLoadModel() throws Exception {
        VersionedEventsModel versionedEventsModel = tenantEventsProvider.getVersionedEventsModel(tenantId);
        eventValidator.validateEvent(versionedEventsModel, mapper.createObjectNode());
    }

    @Test
    public void testModelUpdate() throws IOException {

        ObjectNode instance = mapper.createObjectNode();
        instance.put("value", "value");
        instance.put("ref_field", mapper.convertValue(new ObjectId("Bar", new Id(2)).toStringForm(), JsonNode.class));
        instance.put("refs_field", mapper.convertValue(Arrays.asList(new ObjectId("Baz", new Id(3)).toStringForm()), JsonNode.class));
        instance.put("all_field", mapper.convertValue(Arrays.asList(new ObjectId("Goo", new Id(4)).toStringForm()), JsonNode.class));

        ObjectNode event2 = mapper.createObjectNode();
        jec.setEventId(event2, 1);
        jec.setInstanceNode(event2, "OhLala", instance);

        EventDefinition definition2 = toEvent("OhLala", instance);

        ChainedVersion version2 = new ChainedVersion("1", "2");
        Mockito.when(eventsProvider.getCurrentEventsVersion(tenantId))
            .thenReturn(version, version2);
        Mockito.when(eventsProvider.getEvents(Mockito.any(EventsProcessorId.class)))
            .thenReturn(Arrays.asList(event), Arrays.asList(definition2));

        tenantEventsProvider.loadModel(tenantId);
        VersionedEventsModel versionedEventsModel = tenantEventsProvider.getVersionedEventsModel(tenantId);
        Validated validateEvent = eventValidator.validateEvent(versionedEventsModel, event2);
        Assert.assertFalse(validateEvent.isValid());

        tenantEventsProvider.loadModel(tenantId);

        versionedEventsModel = tenantEventsProvider.getVersionedEventsModel(tenantId);
        validateEvent = eventValidator.validateEvent(versionedEventsModel, event2);
        Assert.assertTrue(validateEvent.isValid());
    }

    private EventDefinition toEvent(String label, Object instance) throws IOException {
        ObjectNode event = toObjectNode(label, instance);
        return EventDefinition.builder(event, true).build();
    }

    private ObjectNode toObjectNode(String label, Object instance) throws IOException {
        ObjectNode convertValue = mapper.convertValue(instance, ObjectNode.class);

        ObjectNode node = mapper.createObjectNode();
        node.put(label, convertValue);
        System.out.println(label);
        System.out.println(mapper.writeValueAsString(node));
        System.out.println();

        return node;
    }
}
