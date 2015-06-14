package com.jivesoftware.os.tasmo.lib;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.jivesoftware.os.jive.utils.id.Id;
import com.jivesoftware.os.jive.utils.id.IdProvider;
import com.jivesoftware.os.jive.utils.id.ObjectId;
import com.jivesoftware.os.jive.utils.id.TenantId;
import com.jivesoftware.os.jive.utils.id.TenantIdAndCentricId;
import com.jivesoftware.os.tasmo.event.api.write.Event;
import com.jivesoftware.os.tasmo.event.api.write.EventWriteException;
import com.jivesoftware.os.tasmo.model.Views;
import java.io.IOException;
import java.util.List;

public interface TasmoMaterializerHarness {

    void initModel(Views views) throws Exception;

    IdProvider idProvider();

    ObjectId write(Event event) throws EventWriteException;

    List<ObjectId> write(List<Event> events) throws EventWriteException;

    void addExpectation(ObjectId rootId, String viewClassName, String viewFieldName, ObjectId[] pathIds, String fieldName, Object value);

    void assertExpectation(TenantIdAndCentricId tenantIdAndCentricId) throws IOException;

    void clearExpectations() throws Exception;

    ObjectNode readView(TenantId tenantId, Id actorId, ObjectId viewId, Id userId) throws Exception;
}
