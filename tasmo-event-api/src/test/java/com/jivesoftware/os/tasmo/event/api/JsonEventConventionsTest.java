package com.jivesoftware.os.tasmo.event.api;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.jivesoftware.os.jive.utils.id.Id;
import com.jivesoftware.os.jive.utils.id.TenantId;
import org.testng.annotations.Test;

/**
 *
 *
 */
public class JsonEventConventionsTest {

    private ObjectMapper objectMapper = new ObjectMapper();
    private JsonEventConventions jsonEventConventions = new JsonEventConventions();

    @Test
    public void testValidateEvent() throws Exception {
        ObjectNode event = createValidEvent();
        jsonEventConventions.validate(event);
        jsonEventConventions.setCausedBy(event, 4_353);
        jsonEventConventions.validate(event);
    }

    @Test (expectedExceptions = IllegalArgumentException.class)
    public void testValidateNoTenantIdField() throws Exception {
        ObjectNode event = createValidEvent();
        event.remove(ReservedFields.TENANT_ID);
        jsonEventConventions.validate(event);
    }

    @Test (expectedExceptions = IllegalArgumentException.class)
    public void testValidateEmptyTenantIdField() throws Exception {
        ObjectNode event = createValidEvent();
        jsonEventConventions.setTenantId(event, new TenantId(""));
        jsonEventConventions.validate(event);
    }

    @Test (expectedExceptions = IllegalArgumentException.class)
    public void testValidateNoActorField() throws Exception {
        ObjectNode event = createValidEvent();
        event.remove(ReservedFields.ACTOR_ID);
        jsonEventConventions.validate(event);
    }

    @Test (expectedExceptions = IllegalArgumentException.class)
    public void testValidateEmptyActorField() throws Exception {
        ObjectNode event = createValidEvent();
        event.put(ReservedFields.ACTOR_ID, "");
        jsonEventConventions.validate(event);
        System.out.println(jsonEventConventions.getActor(event));
    }

    @Test (expectedExceptions = IllegalArgumentException.class)
    public void testValidateInvalidActorField() throws Exception {
        ObjectNode event = createValidEvent();
        event.put(ReservedFields.ACTOR_ID, "InvalidId");
        jsonEventConventions.validate(event);
    }


    @Test (expectedExceptions = IllegalArgumentException.class)
    public void testValidateNoUserIdField() throws Exception {
        ObjectNode event = createValidEvent();
        event.remove(ReservedFields.USER_ID);
        jsonEventConventions.validate(event);
    }

    @Test (expectedExceptions = IllegalArgumentException.class)
    public void testValidateEmptyUserIdField() throws Exception {
        ObjectNode event = createValidEvent();
        event.put(ReservedFields.USER_ID, "");
        jsonEventConventions.validate(event);
        System.out.println(jsonEventConventions.getActor(event));
    }

    @Test (expectedExceptions = IllegalArgumentException.class)
    public void testValidateInvalidUserIdField() throws Exception {
        ObjectNode event = createValidEvent();
        event.put(ReservedFields.USER_ID, "InvalidId");
        jsonEventConventions.validate(event);
    }

    @Test (expectedExceptions = IllegalArgumentException.class)
    public void testValidateNoClassNameField() throws Exception {
        ObjectNode event = createValidEvent();
        event.remove("TestEvent");
        jsonEventConventions.validate(event);
    }

    @Test (expectedExceptions = IllegalArgumentException.class)
    public void testValidateNoInstanceIdField() throws Exception {
        ObjectNode event = createValidEvent();
        ((ObjectNode) event.get("TestEvent")).remove(ReservedFields.INSTANCE_ID);
        jsonEventConventions.validate(event);
    }

    @Test (expectedExceptions = IllegalArgumentException.class)
    public void testValidateNegativeInstanceId() throws Exception {
        ObjectNode event = createValidEvent();
        ((ObjectNode) event.get("TestEvent")).put(ReservedFields.INSTANCE_ID, -1);
        jsonEventConventions.validate(event);
    }

    @Test (expectedExceptions = IllegalArgumentException.class)
    public void testValidateExpectedFieldCountNoCausedBy() throws Exception {
        ObjectNode event = createValidEvent();
        event.put("foobar", "bar");
        jsonEventConventions.validate(event);
    }

    @Test (expectedExceptions = IllegalArgumentException.class)
    public void testValidateExpectedFieldCountWithCausedBy() throws Exception {
        ObjectNode event = createValidEvent();
        event.put("foobar", "bar");
        jsonEventConventions.setCausedBy(event, 4_235);
        jsonEventConventions.validate(event);
    }

    /*
     * The nil field identifier is meant to be used in view model definitions only.  Prevent it from being used elsewhere.
     */
    @Test (expectedExceptions = IllegalArgumentException.class)
    public void testValidateNilNotInEventWrapper() throws Exception {
        ObjectNode event = createValidEvent();
        event.put(ReservedFields.NIL_FIELD, "bar");
        jsonEventConventions.validate(event);
    }

    /*
     * The nil field identifier is meant to be used in view model definitions only.  Prevent it from being used elsewhere.
     */
    @Test (expectedExceptions = IllegalArgumentException.class)
    public void testValidateNilNotInEventPayload() throws Exception {
        ObjectNode event = createValidEvent();
        String className = jsonEventConventions.getInstanceClassName(event);
        ObjectNode instanceNode = jsonEventConventions.getInstanceNode(event, className);
        instanceNode.put(ReservedFields.NIL_FIELD, "bar");
        jsonEventConventions.validate(event);
    }

    private ObjectNode createValidEvent() {
        ObjectNode event = objectMapper.createObjectNode();
        jsonEventConventions.setTenantId(event, new TenantId("foobar"));
        jsonEventConventions.setActorId(event, new Id(123_456));
        jsonEventConventions.setUserId(event, new Id(123_456));
        jsonEventConventions.setInstanceClassName(event, "TestEvent");
        jsonEventConventions.setInstanceId(event, new Id(7_868_763), "TestEvent");
        return event;
    }
}
