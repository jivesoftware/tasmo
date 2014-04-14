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
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 *
 */
public class DeletesWithAdditionalFieldsTest extends BaseTasmoTest {

    @Test
    public void testDeletes() throws Exception {
        String viewClassName = "Values";
        String viewFieldName = "userInfo";
        Expectations expectations = initModelPaths(viewClassName + "::" + viewFieldName + "::User.userName,age");
        ObjectId user1 = write(EventBuilder.create(idProvider, "User", tenantId, actorId).set("userName", "ted").build());

        expectations.addExpectation(user1, viewClassName, viewFieldName, new ObjectId[]{user1}, "userName", "ted");
        expectations.assertExpectation(tenantIdAndCentricId);
        expectations.clear();

        ObjectNode view = readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, user1.getId()));
        Assert.assertNotNull(view);

        write(EventBuilder.update(user1, tenantId, actorId).set(ReservedFields.DELETED, true).set("userName", "frank").build());

        view = readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, user1.getId()));
        Assert.assertNull(view);

    }

}
