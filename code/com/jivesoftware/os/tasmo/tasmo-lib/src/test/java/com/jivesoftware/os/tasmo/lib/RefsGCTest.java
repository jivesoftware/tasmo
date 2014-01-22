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
import java.util.Arrays;
import org.testng.annotations.Test;

/**
 *
 */
public class RefsGCTest extends RefsAsLeafValueTest {

    @Test
    public void testRefsGC() throws Exception {
        String viewClassName = "RefToValues";
        String viewFieldName = "userInfo";
        Expectations expectations = initModelPaths(viewClassName + "::" + viewFieldName + "::Content.refs_users.refs.User|User.userName,age");
        ObjectId user1 = write(EventBuilder.create(idProvider, "User", tenantId, actorId).set("userName", "ted").build());
        ObjectId user2 = write(EventBuilder.create(idProvider, "User", tenantId, actorId).set("userName", "bill").build());
        ObjectId user3 = write(EventBuilder.create(idProvider, "User", tenantId, actorId).set("userName", "jane").build());
        ObjectId user4 = write(EventBuilder.create(idProvider, "User", tenantId, actorId).set("userName", "doe").build());
        ObjectId content1 =
            write(EventBuilder.create(idProvider, "Content", tenantId, actorId).set("refs_users", Arrays.asList(user1, user2)).build());
        expectations.addExpectation(content1, viewClassName, viewFieldName, new ObjectId[]{ content1, user1 }, "userName", "ted");
        expectations.addExpectation(content1, viewClassName, viewFieldName, new ObjectId[]{ content1, user2 }, "userName", "bill");
        expectations.assertExpectation(tenantIdAndCentricId);
        expectations.clear();
        write(EventBuilder.update(content1, tenantId, actorId).set("refs_users", Arrays.asList(user3, user4)).build());
        expectations.addExpectation(content1, viewClassName, viewFieldName, new ObjectId[]{ content1, user3 }, "userName", "jane");
        expectations.addExpectation(content1, viewClassName, viewFieldName, new ObjectId[]{ content1, user4 }, "userName", "doe");
        expectations.assertExpectation(tenantIdAndCentricId);
        expectations.clear();
        ObjectNode view = readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, content1.getId()));
        System.out.println(mapper.writeValueAsString(view));
    }
}
