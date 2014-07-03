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
import org.testng.annotations.Test;

/**
 *
 */
public class RefTest extends BaseTest {

    @Test (dataProvider = "tasmoMaterializer", invocationCount = 1, singleThreaded = true)
    public void testRef(TasmoMaterializerHarness t) throws Exception {
        String viewClassName = "RefToValues";
        String viewFieldName = "userInfo";
        Views views = TasmoModelFactory.modelToViews(viewClassName + "::" + viewFieldName + "::Content.ref_user.ref.User|User.userName,age");
        t.initModel(views);
        ObjectId user1 = t.write(EventBuilder.create(t.idProvider(), "User", tenantId, actorId).set("userName", "ted").build());
        ObjectId content1 = t.write(EventBuilder.create(t.idProvider(), "Content", tenantId, actorId).set("ref_user", user1).build());
        t.addExpectation(content1, viewClassName, viewFieldName, new ObjectId[]{ content1, user1 }, "userName", "ted");
        t.assertExpectation(tenantIdAndCentricId);
        t.clearExpectations();
        ObjectNode view = t.readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, content1.getId()));
        System.out.println(mapper.writeValueAsString(view));
    }

    @Test (dataProvider = "tasmoMaterializer", invocationCount = 1, singleThreaded = true)
    public void testUpdateRef(TasmoMaterializerHarness t) throws Exception {
        String viewClassName = "RefToValues";
        String viewFieldName = "userInfo";
        Views views = TasmoModelFactory.modelToViews(viewClassName + "::" + viewFieldName + "::Content.ref_user.ref.User|User.userName,age");
        t.initModel(views);
        ObjectId user1 = t.write(EventBuilder.create(t.idProvider(), "User", tenantId, actorId).set("userName", "ted").build());
        ObjectId content1 = t.write(EventBuilder.create(t.idProvider(), "Content", tenantId, actorId).set("ref_user", user1).build());
        t.addExpectation(content1, viewClassName, viewFieldName, new ObjectId[]{ content1, user1 }, "userName", "ted");
        t.assertExpectation(tenantIdAndCentricId);
        t.clearExpectations();
        ObjectNode view = t.readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, content1.getId()));
        System.out.println(mapper.writeValueAsString(view));

        ObjectId user2 = t.write(EventBuilder.create(t.idProvider(), "User", tenantId, actorId).set("userName", "jane").build());
        t.write(EventBuilder.update(content1, tenantId, actorId).set("ref_user", user2).build());

        t.addExpectation(content1, viewClassName, viewFieldName, new ObjectId[]{ content1, user2 }, "userName", "jane");
        t.assertExpectation(tenantIdAndCentricId);
        t.clearExpectations();
        view = t.readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, content1.getId()));
        System.out.println(mapper.writeValueAsString(view));
    }
}
