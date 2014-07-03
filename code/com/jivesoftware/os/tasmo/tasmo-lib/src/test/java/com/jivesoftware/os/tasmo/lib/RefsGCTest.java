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
import com.jivesoftware.os.jive.utils.id.ObjectId;
import com.jivesoftware.os.tasmo.event.api.write.EventBuilder;
import com.jivesoftware.os.tasmo.model.Views;
import java.util.Arrays;
import org.testng.annotations.Test;

/**
 *
 */
public class RefsGCTest extends RefsAsLeafValueTest {

    @Test (dataProvider = "tasmoMaterializer", invocationCount = 1, singleThreaded = true)
    public void testRefsGC(TasmoMaterializerHarness t) throws Exception {
        String viewClassName = "RefToValues";
        String viewFieldName = "userInfo";

        Views views = TasmoModelFactory.modelToViews(viewClassName + "::" + viewFieldName + "::Content.refs_users.refs.User|User.userName,age");
        t.initModel(views);
        ObjectId user1 = t.write(EventBuilder.create(t.idProvider(), "User", tenantId, actorId).set("userName", "ted").build());
        ObjectId user2 = t.write(EventBuilder.create(t.idProvider(), "User", tenantId, actorId).set("userName", "bill").build());
        ObjectId user3 = t.write(EventBuilder.create(t.idProvider(), "User", tenantId, actorId).set("userName", "jane").build());
        ObjectId user4 = t.write(EventBuilder.create(t.idProvider(), "User", tenantId, actorId).set("userName", "doe").build());
        ObjectId content1 =
            t.write(EventBuilder.create(t.idProvider(), "Content", tenantId, actorId).set("refs_users", Arrays.asList(user1, user2)).build());
        t.addExpectation(content1, viewClassName, viewFieldName, new ObjectId[]{ content1, user1 }, "userName", "ted");
        t.addExpectation(content1, viewClassName, viewFieldName, new ObjectId[]{ content1, user2 }, "userName", "bill");
        t.assertExpectation(tenantIdAndCentricId);
        t.clearExpectations();
        t.write(EventBuilder.update(content1, tenantId, actorId).set("refs_users", Arrays.asList(user3, user4)).build());
        t.addExpectation(content1, viewClassName, viewFieldName, new ObjectId[]{ content1, user3 }, "userName", "jane");
        t.addExpectation(content1, viewClassName, viewFieldName, new ObjectId[]{ content1, user4 }, "userName", "doe");
        t.assertExpectation(tenantIdAndCentricId);
        t.clearExpectations();
        ObjectNode view = t.readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, content1.getId()));
        System.out.println(mapper.writeValueAsString(view));
    }
}
