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
import org.testng.annotations.Test;

/**
 *
 */
public class CreationAndUpdateTest extends BaseTasmoTest {

    String ContentView = "ContentView";
    String ContainerView = "ContainerView";
    String originalAuthorName = "originalAuthorName";
    String lastName = "lastName";
    String firstName = "firstName";
    String containerName = "containerName";
    String tags = "tags";
    String status = "status";
    String moderatorNames = "moderatorNames";
    String moderatorTags = "moderatorTags";

    @Test
    public void testCreationAndUpdate() throws Exception {
        Expectations expectations = initModelPaths(
            ContentView + "::" + originalAuthorName + "::Content.ref_originalAuthor.ref.User|User.userName",
            ContentView + "::" + lastName + "::Content.ref_originalAuthor.ref.User|User.lastName",
            ContentView + "::" + containerName + "::Content.ref_parent.ref.Container|Container.name",
            ContentView + "::" + tags + "::Content.refs_tags.refs.Tag|Tag.name",
            ContentView + "::" + status + "::Content.status",
            ContentView + "::" + moderatorNames + "::Content.ref_parent.ref.Container|Container.refs_moderators.refs.User|User.userName",
            ContainerView + "::" + moderatorTags + "::Container.refs_moderators.refs.User|User.backRefs.Content.ref_originalAuthor|"
            + "Content.refs_tags.refs.Tag|Tag.name",
            ContentView + "::" + firstName + "::Content.ref_originalAuthor.ref.User|User.firstName");

        //1 - 5
        ObjectId userId = write(EventBuilder.create(idProvider, "User", tenantId, actorId).set("userName", "bob").set("LastName", "brown").build());

        //2 - 6
        ObjectId containerId = write(EventBuilder.create(idProvider, "Container", tenantId, actorId).set("name", "bob-blogs").build());

        //3 - 7
        ObjectId versionedContentId = write(EventBuilder.create(idProvider, "VersionedContent", tenantId, actorId).set("subject", "car vs truck")
            .build());
        //4 - 8
        ObjectId contentId = write(EventBuilder.create(idProvider, "Content", tenantId, actorId).
            set("ref_originalAuthor", userId).
            set("ref_parent", containerId).
            set("ref_versionedContent", versionedContentId).
            build());

        // - 9
        write(EventBuilder.update(userId, tenantId, actorId).set("userName", "robert").build());
        // - 10
        write(EventBuilder.update(userId, tenantId, actorId).set("userName", "bill").build());
        // - 11
        write(EventBuilder.update(containerId, tenantId, actorId).set("name", "kims-blogs").build());

        //12 - 15
        ObjectId tag1 = write(EventBuilder.create(idProvider, "Tag", tenantId, actorId).set("name", "tag1").build());
        //13 - 16
        ObjectId tag2 = write(EventBuilder.create(idProvider, "Tag", tenantId, actorId).set("name", "tag2").build());
        //14 - 17
        ObjectId tag3 = write(EventBuilder.create(idProvider, "Tag", tenantId, actorId).set("name", "tag3").build());
        //  - 18
        write(EventBuilder.update(contentId, tenantId, actorId).set("refs_tags", Arrays.asList(
            tag1,
            tag2,
            tag3)).build());

        // - 19
        write(EventBuilder.update(tag3, tenantId, actorId).set("name", "booya").build());

        // -20
        ObjectId contentId2 = write(EventBuilder.update(contentId, tenantId, actorId).set("status", "draft").build());

        expectations.addExpectation(contentId, ContentView, originalAuthorName, new ObjectId[]{contentId, userId}, "userName", "bill");
        expectations.addExpectation(contentId, ContentView, tags, new ObjectId[]{contentId, tag1}, "name", "tag1");
        expectations.addExpectation(contentId, ContentView, tags, new ObjectId[]{contentId, tag2}, "name", "tag2");
        expectations.addExpectation(contentId, ContentView, tags, new ObjectId[]{contentId, tag3}, "name", "booya");
        expectations.addExpectation(contentId, ContentView, status, new ObjectId[]{contentId}, "status", "draft");

        expectations.assertExpectation(tenantIdAndCentricId);
        expectations.clear();

        // Container.moderators.refs.User|User.backRefs.Content.originalAuthor|Content.tags.refs.Tag|Tag.name

        //21 - 22
        ObjectId userId2 = write(EventBuilder.create(idProvider, "User", tenantId, actorId)
            .set("userName", "mary").set("lastName", "jane").build());
        //- 23
        write(EventBuilder.update(contentId2, tenantId, actorId).set("ref_originalAuthor", userId2).set("refs_tags", Arrays.asList(tag1, tag2, tag3))
            .build());
        // - 23
        write(EventBuilder.update(containerId, tenantId, actorId).set("refs_moderators", Arrays.asList(userId, userId2)).build());

        // - 24
        write(EventBuilder.update(contentId, tenantId, actorId).clear("status").build());
        expectations.addExpectation(contentId, ContentView, status, new ObjectId[]{contentId}, "status", null);
        expectations.assertExpectation(tenantIdAndCentricId);
        expectations.clear();

        ObjectNode view = readView(tenantIdAndCentricId, actorId, new ObjectId(ContainerView, containerId.getId()));
        System.out.println(mapper.writeValueAsString(view));

        view = readView(tenantIdAndCentricId, actorId, new ObjectId(ContentView, contentId.getId()));
        System.out.println(mapper.writeValueAsString(view));

        view = readView(tenantIdAndCentricId, actorId, new ObjectId(ContainerView, containerId.getId()));
        System.out.println(mapper.writeValueAsString(view));
    }
}
