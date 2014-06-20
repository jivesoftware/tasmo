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
public class MultiBackRefTest extends BaseTasmoTest {

    String TaggedContentOtherAuthorsView = "TaggedContentOtherAuthorsView";
    String taggedContentOtherAuthors = "taggedContentOtherAuthors";

    @Test
    public void testMultiBackRef() throws Exception {
        Expectations expectations =
            initModelPaths(TaggedContentOtherAuthorsView + "::" + taggedContentOtherAuthors
            + "::Tag.backRefs.Content.refs_tags|Content.refs_otherAuthors.refs.User|User.userName");
        ObjectId otherAuthorId1 = write(EventBuilder.create(idProvider, "User", tenantId, actorId).set("userName", "ted").build()); //1 - 7
        ObjectId otherAuthorId2 = write(EventBuilder.create(idProvider, "User", tenantId, actorId).set("userName", "john").build()); //2 - 8
        ObjectId contentId1 =
            write(EventBuilder.create(idProvider, "Content", tenantId, actorId)
            .set("refs_otherAuthors", Arrays.asList(otherAuthorId1, otherAuthorId2)).build()); //3 - 9
        ObjectId contentId2 =
            write(EventBuilder.create(idProvider, "Content", tenantId, actorId)
            .set("refs_otherAuthors", Arrays.asList(otherAuthorId1, otherAuthorId2)).build()); //4 - 10
        ObjectId tagId1 = write(EventBuilder.create(idProvider, "Tag", tenantId, actorId).build()); //5 - 11
        ObjectId tagId2 = write(EventBuilder.create(idProvider, "Tag", tenantId, actorId).build()); //6 - 12
        write(EventBuilder.update(contentId1, tenantId, actorId).set("refs_tags", Arrays.asList(tagId1, tagId2)).build()); //  - 13
        write(EventBuilder.update(contentId2, tenantId, actorId).set("refs_tags", Arrays.asList(tagId1, tagId2)).build()); //  - 14
        expectations.addExpectation(tagId1, TaggedContentOtherAuthorsView, taggedContentOtherAuthors, new ObjectId[]{tagId1, contentId1, otherAuthorId1},
            "userName", "ted");
        expectations.addExpectation(tagId1, TaggedContentOtherAuthorsView, taggedContentOtherAuthors, new ObjectId[]{tagId1, contentId1, otherAuthorId2},
            "userName", "john");
        expectations.assertExpectation(tenantIdAndCentricId);
        expectations.addExpectation(tagId1, TaggedContentOtherAuthorsView, taggedContentOtherAuthors, new ObjectId[]{tagId1, contentId2, otherAuthorId1},
            "userName", "ted");
        expectations.addExpectation(tagId1, TaggedContentOtherAuthorsView, taggedContentOtherAuthors, new ObjectId[]{tagId1, contentId2, otherAuthorId2},
            "userName", "john");
        expectations.assertExpectation(tenantIdAndCentricId);
        expectations.addExpectation(tagId2, TaggedContentOtherAuthorsView, taggedContentOtherAuthors, new ObjectId[]{tagId2, contentId1, otherAuthorId1},
            "userName", "ted");
        expectations.addExpectation(tagId2, TaggedContentOtherAuthorsView, taggedContentOtherAuthors, new ObjectId[]{tagId2, contentId1, otherAuthorId2},
            "userName", "john");
        expectations.assertExpectation(tenantIdAndCentricId);
        expectations.addExpectation(tagId2, TaggedContentOtherAuthorsView, taggedContentOtherAuthors, new ObjectId[]{tagId2, contentId2, otherAuthorId1},
            "userName", "ted");
        expectations.addExpectation(tagId2, TaggedContentOtherAuthorsView, taggedContentOtherAuthors, new ObjectId[]{tagId2, contentId2, otherAuthorId2},
            "userName", "john");
        expectations.assertExpectation(tenantIdAndCentricId);
        expectations.clear();
        ObjectNode view1 = readView(tenantIdAndCentricId, actorId, new ObjectId(TaggedContentOtherAuthorsView, tagId1.getId()));
        System.out.println(mapper.writeValueAsString(view1));
        ObjectNode view2 = readView(tenantIdAndCentricId, actorId, new ObjectId(TaggedContentOtherAuthorsView, tagId2.getId()));
        System.out.println(mapper.writeValueAsString(view2));
    }
}
