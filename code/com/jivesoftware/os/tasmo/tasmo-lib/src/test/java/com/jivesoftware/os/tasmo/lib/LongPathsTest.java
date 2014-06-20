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
import java.util.Arrays;
import java.util.Collections;
import org.testng.annotations.Test;

/**
 *
 */
public class LongPathsTest extends BaseTasmoTest {

    String ContentView = "ContentView";
    String moderatorNames = "moderatorNames";

    @Test
    public void testLongPath() throws Exception {
        Expectations expectations = initModelPaths(ContentView + "::" + moderatorNames
                + "::Content.ref_parent.ref.Container|Container.refs_moderators.refs.User|User.userName");
        ObjectId userId = write(EventBuilder.create(idProvider, "User", tenantId, actorId).set("userName", "moderator").build());
        ObjectId containerId = write(EventBuilder.create(idProvider, "Container", tenantId, actorId).set("name", "moderated container").build());
        write(EventBuilder.update(containerId, tenantId, actorId).set("refs_moderators", Arrays.asList(userId)).build());
        ObjectId contentId = write(EventBuilder.create(idProvider, "Content", tenantId, actorId).set("ref_parent", containerId).build());
        expectations.addExpectation(contentId, ContentView, moderatorNames, new ObjectId[]{contentId, containerId, userId}, "userName", "moderator");
        expectations.assertExpectation(tenantIdAndCentricId);
        expectations.clear();
        write(EventBuilder.update(containerId, tenantId, actorId).set("refs_moderators", Collections.<ObjectId>emptyList()).build());
        expectations.addExpectation(contentId, ContentView, moderatorNames, new ObjectId[]{contentId, containerId, userId}, "userName", null);
        expectations.assertExpectation(tenantIdAndCentricId);
        expectations.clear();
        ObjectId userId2 = write(EventBuilder.create(idProvider, "User", tenantId, actorId).set("userName", "moderator2").build());
        write(EventBuilder.update(containerId, tenantId, actorId).set("refs_moderators", Arrays.asList(userId, userId2)).build());
        expectations.addExpectation(contentId, ContentView, moderatorNames, new ObjectId[]{contentId, containerId, userId}, "userName", "moderator");
        expectations.addExpectation(contentId, ContentView, moderatorNames, new ObjectId[]{contentId, containerId, userId2}, "userName", "moderator2");
        expectations.assertExpectation(tenantIdAndCentricId);
        expectations.clear();

        ObjectNode view = readView(tenantIdAndCentricId, actorId, new ObjectId(ContentView, contentId.getId()));
        System.out.println("Pre-event:" + mapper.writeValueAsString(view));

        write(EventBuilder.update(containerId, tenantId, actorId).set("refs_moderators", Arrays.asList(userId2)).build());

        view = readView(tenantIdAndCentricId, actorId, new ObjectId(ContentView, contentId.getId()));
        System.out.println("Post-event:" + mapper.writeValueAsString(view));
        expectations.addExpectation(contentId, ContentView, moderatorNames, new ObjectId[]{contentId, containerId, userId}, "userName", null);
        expectations.addExpectation(contentId, ContentView, moderatorNames, new ObjectId[]{contentId, containerId, userId2}, "userName", "moderator2");
        expectations.assertExpectation(tenantIdAndCentricId);
        expectations.clear();

        view = readView(tenantIdAndCentricId, actorId, new ObjectId(ContentView, contentId.getId()));
        System.out.println(mapper.writeValueAsString(view));
    }
}
