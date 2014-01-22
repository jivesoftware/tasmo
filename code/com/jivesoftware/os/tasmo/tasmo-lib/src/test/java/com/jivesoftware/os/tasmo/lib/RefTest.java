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
import com.jivesoftware.os.tasmo.event.api.write.EventBuilder;
import com.jivesoftware.os.tasmo.id.ObjectId;
import org.testng.annotations.Test;

/**
 *
 */
public class RefTest extends BaseTasmoTest {

    @Test
    public void testRef() throws Exception {
        String viewClassName = "RefToValues";
        String viewFieldName = "userInfo";
        Expectations expectations = initModelPaths(viewClassName + "::" + viewFieldName + "::Content.ref_user.ref.User|User.userName,age");
        ObjectId user1 = write(EventBuilder.create(idProvider, "User", tenantId, actorId).set("userName", "ted").build());
        ObjectId content1 = write(EventBuilder.create(idProvider, "Content", tenantId, actorId).set("ref_user", user1).build());
        expectations.addExpectation(content1, viewClassName, viewFieldName, new ObjectId[]{ content1, user1 }, "userName", "ted");
        expectations.assertExpectation(tenantIdAndCentricId);
        expectations.clear();
        ObjectNode view = readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, user1.getId()));
        System.out.println(mapper.writeValueAsString(view));
    }
}
