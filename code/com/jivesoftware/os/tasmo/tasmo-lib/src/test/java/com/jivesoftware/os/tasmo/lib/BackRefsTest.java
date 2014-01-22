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
public class BackRefsTest extends BaseTasmoTest {

    @Test
    public void testBackRefs() throws Exception {
        String viewClassName = "BackRefs";
        String viewFieldName = "contentsUsers";
        Expectations expectations = initModelPaths(viewClassName + "::" + viewFieldName + "::Content.backRefs.User.refs_contents|User.userName");
        ObjectId contentId1 = write(EventBuilder.create(idProvider, "Content", tenantId, actorId).set("name", "content1").build());
        ObjectId contentId2 = write(EventBuilder.create(idProvider, "Content", tenantId, actorId).set("name", "content2").build());
        ObjectId otherAuthorId1 =
            write(EventBuilder.create(idProvider, "User", tenantId, actorId).set("userName", "ted")
            .set("refs_contents", Arrays.asList(contentId1, contentId2)).build());
        expectations.addExpectation(contentId1, viewClassName, viewFieldName, new ObjectId[]{ contentId1, otherAuthorId1 }, "userName", "ted");
        expectations.addExpectation(contentId2, viewClassName, viewFieldName, new ObjectId[]{ contentId2, otherAuthorId1 }, "userName", "ted");
        expectations.assertExpectation(tenantIdAndCentricId);
        expectations.clear();
        ObjectNode view = readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, contentId1.getId()));
        System.out.println(mapper.writeValueAsString(view));
        ObjectId otherAuthorId2 =
            write(EventBuilder.create(idProvider, "User", tenantId, actorId).set("userName", "john")
            .set("refs_contents", Arrays.asList(contentId1, contentId2)).build());
        expectations.addExpectation(contentId1, viewClassName, viewFieldName, new ObjectId[]{ contentId1, otherAuthorId2 }, "userName", "john");
        expectations.addExpectation(contentId2, viewClassName, viewFieldName, new ObjectId[]{ contentId2, otherAuthorId2 }, "userName", "john");
        expectations.assertExpectation(tenantIdAndCentricId);
        expectations.clear();
        view = readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, contentId2.getId()));
        System.out.println(mapper.writeValueAsString(view));
    }
}
