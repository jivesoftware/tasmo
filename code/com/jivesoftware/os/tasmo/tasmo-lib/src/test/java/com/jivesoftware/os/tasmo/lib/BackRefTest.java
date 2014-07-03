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
public class BackRefTest extends BaseTest {

    @Test (dataProvider = "tasmoMaterializer", invocationCount = 1, singleThreaded = true)
    public void testBackRef(TasmoMaterializerHarness t) throws Exception {
        String viewClassName = "RefToValues";
        String viewFieldName = "userInfo";
        Views views = TasmoModelFactory.modelToViews(viewClassName + "::" + viewFieldName + "::Content.backRefs.User.ref_content|User.userName,age");
        t.initModel(views);

        ObjectId content1 = t.write(EventBuilder.create(t.idProvider(), "Content", tenantId, actorId).set("contentName", "foo").build());
        ObjectId user1 =
            t.write(EventBuilder.create(t.idProvider(), "User", tenantId, actorId).set("userName", "ted").set("ref_content", content1).build());
        t.addExpectation(content1, viewClassName, viewFieldName, new ObjectId[]{ content1, user1 }, "userName", "ted");
        ObjectNode view = t.readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, content1.getId()));
        t.assertExpectation(tenantIdAndCentricId);
        t.clearExpectations();
        System.out.println(mapper.writeValueAsString(view));
    }
}
