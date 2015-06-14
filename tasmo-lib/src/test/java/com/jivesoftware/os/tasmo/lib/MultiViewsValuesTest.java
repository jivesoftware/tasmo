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
import com.jivesoftware.os.jive.utils.id.Id;
import com.jivesoftware.os.jive.utils.id.ObjectId;
import com.jivesoftware.os.tasmo.event.api.write.EventBuilder;
import com.jivesoftware.os.tasmo.model.Views;
import org.testng.annotations.Test;

/**
 *
 */
public class MultiViewsValuesTest extends BaseTest {

    @Test (dataProvider = "tasmoMaterializer", invocationCount = 1, singleThreaded = true)
    public void testValues(TasmoMaterializerHarness t) throws Exception {
        String viewClassName1 = "Values1";
        String viewFieldName1 = "userInfo1";

        String viewClassName2 = "Values2";
        String viewFieldName2 = "userInfo2";

        String viewClassName3 = "Values3";
        String viewFieldName3 = "userInfo3";

        Views views = TasmoModelFactory.modelToViews(
            viewClassName1 + "::" + viewFieldName1 + "::User.userName,age",
            viewClassName2 + "::" + viewFieldName2 + "::User.userName,age",
            viewClassName3 + "::" + viewFieldName3 + "::User.userName,age");
        t.initModel(views);

        ObjectId user1 = t.write(
            EventBuilder.create(t.idProvider(), "User", tenantId, actorId)
            .set("userName", "ted")
            .build());

        ObjectNode view = t.readView(tenantId, actorId, new ObjectId(viewClassName1, user1.getId()), Id.NULL);
        System.out.println(mapper.writeValueAsString(view));

        view = t.readView(tenantId, actorId, new ObjectId(viewClassName2, user1.getId()), Id.NULL);
        System.out.println(mapper.writeValueAsString(view));

        view = t.readView(tenantId, actorId, new ObjectId(viewClassName3, user1.getId()), Id.NULL);
        System.out.println(mapper.writeValueAsString(view));


        t.addExpectation(user1, viewClassName1, viewFieldName1, new ObjectId[]{ user1 }, "userName", "ted");
        t.addExpectation(user1, viewClassName2, viewFieldName2, new ObjectId[]{ user1 }, "userName", "ted");
        t.addExpectation(user1, viewClassName3, viewFieldName3, new ObjectId[]{ user1 }, "userName", "ted");
        t.assertExpectation(tenantIdAndCentricId);
        t.clearExpectations();


    }
}
