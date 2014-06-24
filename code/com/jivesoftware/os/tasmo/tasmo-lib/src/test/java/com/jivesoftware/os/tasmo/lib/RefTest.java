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
import com.jivesoftware.os.tasmo.lib.write.PathId;
import junit.framework.Assert;
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
        ObjectNode view = readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, content1.getId()));
        System.out.println(mapper.writeValueAsString(view));

        ObjectNode materializedView = readMaterializeView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, content1.getId()));
        System.out.println("-------------------------");
        System.out.println(mapper.writeValueAsString(view));

        Assert.assertEquals(view, materializedView);

    }

    @Test
    public void testUpdateRef() throws Exception {
        String viewClassName = "RefToValues";
        String viewFieldName = "userInfo";
        Expectations expectations = initModelPaths(viewClassName + "::" + viewFieldName + "::Content.ref_user.ref.User|User.userName,age");
        ObjectId user1 = write(EventBuilder.create(idProvider, "User", tenantId, actorId).set("userName", "ted").build());
        ObjectId content1 = write(EventBuilder.create(idProvider, "Content", tenantId, actorId).set("ref_user", user1).build());
        expectations.addExpectation(content1, viewClassName, viewFieldName, new ObjectId[]{ content1, user1 }, "userName", "ted");
        expectations.assertExpectation(tenantIdAndCentricId);
        expectations.clear();
        ObjectNode view = readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, content1.getId()));
        System.out.println(mapper.writeValueAsString(view));

        ObjectId user2 = write(EventBuilder.create(idProvider, "User", tenantId, actorId).set("userName", "jane").build());
        write(EventBuilder.update(content1, tenantId, actorId).set("ref_user", user2).build());

        expectations.addExpectation(content1, viewClassName, viewFieldName, new ObjectId[]{ content1, user2 }, "userName", "jane");
        expectations.assertExpectation(tenantIdAndCentricId);
        expectations.clear();
        view = readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, content1.getId()));
        System.out.println(mapper.writeValueAsString(view));
    }
}
