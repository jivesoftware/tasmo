/*
 * $Revision$
 * $Date$
 *
 * Copyright (C) 1999-$year$ Jive Software. All rights reserved.
 *
 * This software is the proprietary information of Jive Software. Use is subject to license terms.
 */
package com.jivesoftware.os.tasmo.lib;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.jivesoftware.os.tasmo.event.api.ReservedFields;
import com.jivesoftware.os.tasmo.event.api.write.EventBuilder;
import com.jivesoftware.os.tasmo.id.ObjectId;
import org.testng.annotations.Test;

/**
 *
 */
public class NilFieldTest extends BaseTasmoTest {

    private static final String NIL = ReservedFields.NIL_FIELD;

    /**
     * Ensure nil field value is stored and retrieved properly.
     */
    @Test
    public void testRootHasNilFieldOnly() throws Exception {
        String viewClassName = "NilOnly";
        String viewFieldName = "nilField";
        Expectations expectations = initModelPaths(viewClassName + "::" + viewFieldName + "::Content." + NIL);
        ObjectId content1 = write(EventBuilder.create(idProvider, "Content", tenantId, actorId).build());
        expectations.addExpectation(content1, viewClassName, viewFieldName, new ObjectId[]{ content1 }, NIL, 0);
        expectations.assertExpectation(tenantIdAndCentricId);
        expectations.clear();
        ObjectNode view = readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, content1.getId()));
        System.out.println(mapper.writeValueAsString(view));
    }

    /**
     * Ensure attempts to bind to instanceId is forbidden in the leaves.
     */
    @Test(expectedExceptions = IllegalStateException.class)
    public void testAttemptedBindToInstanceId() throws Exception {
        String viewClassName = "InstanceIdBinding";
        initModelPaths(viewClassName + "::" + ReservedFields.INSTANCE_ID + "::Content." + ReservedFields.INSTANCE_ID);
    }

}
