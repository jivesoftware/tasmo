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
import java.util.Arrays;
import org.testng.annotations.Test;

/**
 *
 */
public class BackRefsTest extends BaseTest {

    @Test (dataProvider = "tasmoMaterializer", invocationCount = 1, singleThreaded = true)
    public void testBackRefs(TasmoMaterializerHarness t) throws Exception {
        String viewClassName = "BackRefs";
        String viewFieldName = "contentsUsers";
        Views views = TasmoModelFactory.modelToViews(viewClassName + "::" + viewFieldName + "::Content.backRefs.User.refs_contents|User.userName");
        t.initModel(views);

        ObjectId contentId1 = t.write(EventBuilder.create(t.idProvider(), "Content", tenantId, actorId).set("name", "content1").build());
        ObjectId contentId2 = t.write(EventBuilder.create(t.idProvider(), "Content", tenantId, actorId).set("name", "content2").build());
        ObjectId otherAuthorId1 =
            t.write(EventBuilder.create(t.idProvider(), "User", tenantId, actorId).set("userName", "ted")
            .set("refs_contents", Arrays.asList(contentId1, contentId2)).build());
        t.addExpectation(contentId1, viewClassName, viewFieldName, new ObjectId[]{ contentId1, otherAuthorId1 }, "userName", "ted");
        t.addExpectation(contentId2, viewClassName, viewFieldName, new ObjectId[]{ contentId2, otherAuthorId1 }, "userName", "ted");

        ObjectNode  view = t.readView(tenantId, actorId, new ObjectId(viewClassName, contentId1.getId()), Id.NULL);
        view = t.readView(tenantId, actorId, new ObjectId(viewClassName, contentId2.getId()), Id.NULL);
        t.assertExpectation(tenantIdAndCentricId);

        t.clearExpectations();
        view = t.readView(tenantId, actorId, new ObjectId(viewClassName, contentId1.getId()), Id.NULL);
        System.out.println(mapper.writeValueAsString(view));
        ObjectId otherAuthorId2 =
            t.write(EventBuilder.create(t.idProvider(), "User", tenantId, actorId).set("userName", "john")
            .set("refs_contents", Arrays.asList(contentId1, contentId2)).build());
        t.addExpectation(contentId1, viewClassName, viewFieldName, new ObjectId[]{ contentId1, otherAuthorId2 }, "userName", "john");
        t.addExpectation(contentId2, viewClassName, viewFieldName, new ObjectId[]{ contentId2, otherAuthorId2 }, "userName", "john");
        view = t.readView(tenantId, actorId, new ObjectId(viewClassName, contentId1.getId()), Id.NULL);
        view = t.readView(tenantId, actorId, new ObjectId(viewClassName, contentId2.getId()), Id.NULL);
        t.assertExpectation(tenantIdAndCentricId);
        t.clearExpectations();
        view = t.readView(tenantId, actorId, new ObjectId(viewClassName, contentId2.getId()), Id.NULL);
        System.out.println(mapper.writeValueAsString(view));
    }
}
