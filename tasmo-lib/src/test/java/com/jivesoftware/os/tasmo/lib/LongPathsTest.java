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
import java.util.Collections;
import org.testng.annotations.Test;

/**
 *
 */
public class LongPathsTest extends BaseTest {

    String contentView = "ContentView";
    String moderatorNames = "moderatorNames";

    @Test (dataProvider = "tasmoMaterializer", invocationCount = 1, singleThreaded = true)
    public void testLongPath(TasmoMaterializerHarness t) throws Exception {
        Views views = TasmoModelFactory.modelToViews(contentView + "::" + moderatorNames
                + "::Content.ref_parent.ref.Container|Container.refs_moderators.refs.User|User.userName");
        t.initModel(views);

        ObjectId userId = t.write(EventBuilder.create(t.idProvider(), "User", tenantId, actorId).set("userName", "moderator").build());
        ObjectId containerId = t.write(EventBuilder.create(t.idProvider(), "Container", tenantId, actorId).set("name", "moderated container").build());
        t.write(EventBuilder.update(containerId, tenantId, actorId).set("refs_moderators", Arrays.asList(userId)).build());
        ObjectId contentId = t.write(EventBuilder.create(t.idProvider(), "Content", tenantId, actorId).set("ref_parent", containerId).build());
        t.addExpectation(contentId, contentView, moderatorNames, new ObjectId[]{contentId, containerId, userId}, "userName", "moderator");
        t.readView(tenantId, actorId, new ObjectId(contentView, contentId.getId()), Id.NULL);
        t.assertExpectation(tenantIdAndCentricId);
        t.clearExpectations();

        t.write(EventBuilder.update(containerId, tenantId, actorId).set("refs_moderators", Collections.<ObjectId>emptyList()).build());
        t.addExpectation(contentId, contentView, moderatorNames, new ObjectId[]{contentId, containerId, userId}, "userName", null);
        t.readView(tenantId, actorId, new ObjectId(contentView, contentId.getId()), Id.NULL);
        t.assertExpectation(tenantIdAndCentricId);
        t.clearExpectations();

        ObjectId userId2 = t.write(EventBuilder.create(t.idProvider(), "User", tenantId, actorId).set("userName", "moderator2").build());
        t.write(EventBuilder.update(containerId, tenantId, actorId).set("refs_moderators", Arrays.asList(userId, userId2)).build());
        t.addExpectation(contentId, contentView, moderatorNames, new ObjectId[]{contentId, containerId, userId}, "userName", "moderator");
        t.addExpectation(contentId, contentView, moderatorNames, new ObjectId[]{contentId, containerId, userId2}, "userName", "moderator2");
        t.readView(tenantId, actorId, new ObjectId(contentView, contentId.getId()), Id.NULL);
        t.assertExpectation(tenantIdAndCentricId);
        t.clearExpectations();

        System.out.println("-----------------------");
        t.write(EventBuilder.update(containerId, tenantId, actorId).set("refs_moderators", Arrays.asList(userId2)).build());
        System.out.println("-----------------------");

        t.addExpectation(contentId, contentView, moderatorNames, new ObjectId[]{contentId, containerId, userId}, "userName", null);
        t.addExpectation(contentId, contentView, moderatorNames, new ObjectId[]{contentId, containerId, userId2}, "userName", "moderator2");

        System.out.println("READ");
        ObjectNode view = t.readView(tenantId, actorId, new ObjectId(contentView, contentId.getId()), Id.NULL);
        System.out.println("Post-event:" + mapper.writeValueAsString(view));
        t.assertExpectation(tenantIdAndCentricId);
        t.clearExpectations();

        view = t.readView(tenantId, actorId, new ObjectId(contentView, contentId.getId()), Id.NULL);
        System.out.println(mapper.writeValueAsString(view));
    }
}
