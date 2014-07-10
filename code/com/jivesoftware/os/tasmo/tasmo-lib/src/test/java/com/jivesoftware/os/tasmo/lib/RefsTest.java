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
public class RefsTest extends BaseTest {

    @Test (dataProvider = "tasmoMaterializer", invocationCount = 1, singleThreaded = true)
    public void testRefs(TasmoMaterializerHarness t) throws Exception {
        String viewClassName = "RefToValues";
        String viewFieldName = "userInfo";
        Views views = TasmoModelFactory.modelToViews(viewClassName + "::" + viewFieldName + "::Content.refs_users.refs.User|User.userName,age");
        t.initModel(views);

        ObjectId user1 = t.write(EventBuilder.create(t.idProvider(), "User", tenantId, actorId).set("userName", "ted").build());
        ObjectId user2 = t.write(EventBuilder.create(t.idProvider(), "User", tenantId, actorId).set("userName", "bill").build());
        ObjectId content1 = t.write(EventBuilder.create(t.idProvider(), "Content", tenantId, actorId).set("refs_users", Arrays.asList(user1, user2)).build());

        t.readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, content1.getId()));
        t.addExpectation(content1, viewClassName, viewFieldName, new ObjectId[]{ content1, user1 }, "userName", "ted");
        t.addExpectation(content1, viewClassName, viewFieldName, new ObjectId[]{ content1, user2 }, "userName", "bill");

        ObjectNode view = t.readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, content1.getId()));
        t.readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, content1.getId()));
        t.assertExpectation(tenantIdAndCentricId);
        t.clearExpectations();

        String deserializationInput = mapper.writeValueAsString(view);
        System.out.println("Input:" + deserializationInput);
    }

    @Test (dataProvider = "tasmoMaterializer", invocationCount = 1, singleThreaded = true)
    public void testMultiplePath(TasmoMaterializerHarness t) throws Exception {
        String viewClassName = "ViewClassName";
        String viewFieldName = "ViewFieldName";
        String viewFieldName2 = "ViewFieldName2";
        Views views = TasmoModelFactory.modelToViews(viewClassName + "::" + viewFieldName + "::Content.refs_users.refs.User|User.userName,age",
            viewClassName + "::" + viewFieldName2 + "::Content.refs_users.refs.User|User.avatar.ref.Avatar|Avatar.creationDate");
        t.initModel(views);
        ObjectId user1 = t.write(EventBuilder.create(t.idProvider(), "User", tenantId, actorId).set("userName", "ted").build());
        ObjectId user2 = t.write(EventBuilder.create(t.idProvider(), "User", tenantId, actorId).set("userName", "bill").build());
        ObjectId content1 =
             t.write(EventBuilder.create(t.idProvider(), "Content", tenantId, actorId).set("refs_users", Arrays.asList(user1, user2)).build());

        t.readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, content1.getId()));
        t.addExpectation(content1, viewClassName, viewFieldName, new ObjectId[]{ content1, user1 }, "userName", "ted");
        t.addExpectation(content1, viewClassName, viewFieldName, new ObjectId[]{ content1, user2 }, "userName", "bill");
        t.assertExpectation(tenantIdAndCentricId);
        t.clearExpectations();

        ObjectId avatar = t.write(EventBuilder.create(t.idProvider(), "Avatar", tenantId, actorId).set("creationDate", "someday").build());
        t.write(EventBuilder.update(user1, tenantId, actorId).set("avatar", avatar).build());

        t.readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, content1.getId()));
        t.addExpectation(content1, viewClassName, viewFieldName, new ObjectId[]{ content1, user1 }, "userName", "ted");
        t.addExpectation(content1, viewClassName, viewFieldName, new ObjectId[]{ content1, user2 }, "userName", "bill");
        t.addExpectation(content1, viewClassName, viewFieldName2, new ObjectId[]{ content1, user1, avatar }, "creationDate", "someday");
        t.assertExpectation(tenantIdAndCentricId);
        t.clearExpectations();

    }

    @Test (dataProvider = "tasmoMaterializer", invocationCount = 1, singleThreaded = true)
    public void testDupeInput(TasmoMaterializerHarness t) throws Exception {
        String viewClassName = "ViewClassName";
        String viewFieldName = "ViewFieldName";
        Views views = TasmoModelFactory.modelToViews(viewClassName + "::" + viewFieldName + "::Content.refs_users.refs.User|User.userName,age");
        t.initModel(views);

        ObjectId user1 = t.write(EventBuilder.create(t.idProvider(), "User", tenantId, actorId).set("userName", "ted").build());
        ObjectId user2 = t.write(EventBuilder.create(t.idProvider(), "User", tenantId, actorId).set("userName", "bill").build());
        ObjectId content1 =
             t.write(EventBuilder.create(t.idProvider(), "Content", tenantId, actorId).set("refs_users", Arrays.asList(user1, user2)).build());

        t.addExpectation(content1, viewClassName, viewFieldName, new ObjectId[]{ content1, user1 }, "userName", "ted");
        t.addExpectation(content1, viewClassName, viewFieldName, new ObjectId[]{ content1, user2 }, "userName", "bill");
        t.readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, content1.getId()));
        t.assertExpectation(tenantIdAndCentricId);
        t.clearExpectations();

        t.write(EventBuilder.update(content1, tenantId, actorId).set("refs_users", Arrays.asList(user1, user2)).build());

        t.addExpectation(content1, viewClassName, viewFieldName, new ObjectId[]{ content1, user1 }, "userName", "ted");
        t.addExpectation(content1, viewClassName, viewFieldName, new ObjectId[]{ content1, user2 }, "userName", "bill");
        t.readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, content1.getId()));
        t.assertExpectation(tenantIdAndCentricId);
        t.clearExpectations();

    }

    @Test (dataProvider = "tasmoMaterializer", invocationCount = 1, singleThreaded = true)
    public void testRefsModification(TasmoMaterializerHarness t) throws Exception {
        String viewClassName = "ViewClassName";
        String viewFieldName = "ViewFieldName";
        Views views = TasmoModelFactory.modelToViews(viewClassName + "::" + viewFieldName + "::Content.refs_users.refs.User|User.userName,age");
        t.initModel(views);

        ObjectId user1 = t.write(EventBuilder.create(t.idProvider(), "User", tenantId, actorId).set("userName", "ted").build());
        ObjectId user2 = t.write(EventBuilder.create(t.idProvider(), "User", tenantId, actorId).set("userName", "bill").build());
        ObjectId user3 = t.write(EventBuilder.create(t.idProvider(), "User", tenantId, actorId).set("userName", "john").build());
        ObjectId content1 =
             t.write(EventBuilder.create(t.idProvider(), "Content", tenantId, actorId).set("refs_users", Arrays.asList(user1, user2)).build());

        t.addExpectation(content1, viewClassName, viewFieldName, new ObjectId[]{ content1, user1 }, "userName", "ted");
        t.addExpectation(content1, viewClassName, viewFieldName, new ObjectId[]{ content1, user2 }, "userName", "bill");
        t.readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, content1.getId()));
        t.assertExpectation(tenantIdAndCentricId);
        t.clearExpectations();

        t.write(EventBuilder.update(content1, tenantId, actorId).set("refs_users", Arrays.asList(user1, user2)).build());

        t.addExpectation(content1, viewClassName, viewFieldName, new ObjectId[]{ content1, user1 }, "userName", "ted");
        t.addExpectation(content1, viewClassName, viewFieldName, new ObjectId[]{ content1, user2 }, "userName", "bill");
        t.readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, content1.getId()));
        t.assertExpectation(tenantIdAndCentricId);
        t.clearExpectations();

        t.write(EventBuilder.update(content1, tenantId, actorId).set("refs_users", Arrays.asList(user1, user3)).build());

        t.addExpectation(content1, viewClassName, viewFieldName, new ObjectId[]{ content1, user1 }, "userName", "ted");
        t.addExpectation(content1, viewClassName, viewFieldName, new ObjectId[]{ content1, user2 }, "userName", null);
        t.addExpectation(content1, viewClassName, viewFieldName, new ObjectId[]{ content1, user3 }, "userName", "john");
        t.readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, content1.getId()));
        t.assertExpectation(tenantIdAndCentricId);
        t.clearExpectations();

    }
}
