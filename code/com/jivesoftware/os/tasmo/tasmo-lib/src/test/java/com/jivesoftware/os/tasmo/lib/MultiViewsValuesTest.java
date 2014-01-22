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
public class MultiViewsValuesTest extends BaseTasmoTest {

    @Test
    public void testValues() throws Exception {
        String viewClassName1 = "Values1";
        String viewFieldName1 = "userInfo1";

        String viewClassName2 = "Values2";
        String viewFieldName2 = "userInfo2";

        String viewClassName3 = "Values3";
        String viewFieldName3 = "userInfo3";

        Expectations expectations = initModelPaths(
            viewClassName1 + "::" + viewFieldName1 + "::User.userName,age",
            viewClassName2 + "::" + viewFieldName2 + "::User.userName,age",
            viewClassName3 + "::" + viewFieldName3 + "::User.userName,age");

        ObjectId user1 = write(
            EventBuilder.create(idProvider, "User", tenantId, actorId)
            .set("userName", "ted")
            .build());

        ObjectNode view = readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName1, user1.getId()));
        System.out.println(mapper.writeValueAsString(view));

        view = readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName2, user1.getId()));
        System.out.println(mapper.writeValueAsString(view));

        view = readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName3, user1.getId()));
        System.out.println(mapper.writeValueAsString(view));


        expectations.addExpectation(user1, viewClassName1, viewFieldName1, new ObjectId[]{ user1 }, "userName", "ted");
        expectations.addExpectation(user1, viewClassName2, viewFieldName2, new ObjectId[]{ user1 }, "userName", "ted");
        expectations.addExpectation(user1, viewClassName3, viewFieldName3, new ObjectId[]{ user1 }, "userName", "ted");
        expectations.assertExpectation(tenantIdAndCentricId);
        expectations.clear();


    }
}
