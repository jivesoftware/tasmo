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
public class MultiBackRefTest extends BaseTest {

    String TaggedContentOtherAuthorsView = "TaggedContentOtherAuthorsView";
    String taggedContentOtherAuthors = "taggedContentOtherAuthors";

    @Test (dataProvider = "tasmoMaterializer", invocationCount = 1, singleThreaded = true)
    public void testMultiBackRef(TasmoMaterializerHarness t) throws Exception {
        Views views = TasmoModelFactory.modelToViews(TaggedContentOtherAuthorsView + "::" + taggedContentOtherAuthors
            + "::Tag.backRefs.Content.refs_tags|Content.refs_otherAuthors.refs.User|User.userName");
        t.initModel(views);

        ObjectId otherAuthorId1 = t.write(EventBuilder.create(t.idProvider(), "User", tenantId, actorId).set("userName", "ted").build()); //1 - 7
        ObjectId otherAuthorId2 = t.write(EventBuilder.create(t.idProvider(), "User", tenantId, actorId).set("userName", "john").build()); //2 - 8
        ObjectId contentId1 =
            t.write(EventBuilder.create(t.idProvider(), "Content", tenantId, actorId)
            .set("refs_otherAuthors", Arrays.asList(otherAuthorId1, otherAuthorId2)).build()); //3 - 9
        ObjectId contentId2 =
            t.write(EventBuilder.create(t.idProvider(), "Content", tenantId, actorId)
            .set("refs_otherAuthors", Arrays.asList(otherAuthorId1, otherAuthorId2)).build()); //4 - 10
        ObjectId tagId1 = t.write(EventBuilder.create(t.idProvider(), "Tag", tenantId, actorId).build()); //5 - 11
        ObjectId tagId2 = t.write(EventBuilder.create(t.idProvider(), "Tag", tenantId, actorId).build()); //6 - 12
        t.write(EventBuilder.update(contentId1, tenantId, actorId).set("refs_tags", Arrays.asList(tagId1, tagId2)).build()); //  - 13
        t.write(EventBuilder.update(contentId2, tenantId, actorId).set("refs_tags", Arrays.asList(tagId1, tagId2)).build()); //  - 14
        ObjectNode view1 = t.readView(tenantId, actorId, new ObjectId(TaggedContentOtherAuthorsView, tagId1.getId()), Id.NULL);

        t.addExpectation(tagId1, TaggedContentOtherAuthorsView, taggedContentOtherAuthors, new ObjectId[]{tagId1, contentId1, otherAuthorId1},
            "userName", "ted");
        t.addExpectation(tagId1, TaggedContentOtherAuthorsView, taggedContentOtherAuthors, new ObjectId[]{tagId1, contentId1, otherAuthorId2},
            "userName", "john");
        t.readView(tenantId, actorId, new ObjectId(TaggedContentOtherAuthorsView, tagId1.getId()), Id.NULL);
        t.assertExpectation(tenantIdAndCentricId);
        t.addExpectation(tagId1, TaggedContentOtherAuthorsView, taggedContentOtherAuthors, new ObjectId[]{tagId1, contentId2, otherAuthorId1},
            "userName", "ted");
        t.addExpectation(tagId1, TaggedContentOtherAuthorsView, taggedContentOtherAuthors, new ObjectId[]{tagId1, contentId2, otherAuthorId2},
            "userName", "john");
        t.readView(tenantId, actorId, new ObjectId(TaggedContentOtherAuthorsView, tagId1.getId()), Id.NULL);
        t.assertExpectation(tenantIdAndCentricId);
        t.addExpectation(tagId2, TaggedContentOtherAuthorsView, taggedContentOtherAuthors, new ObjectId[]{tagId2, contentId1, otherAuthorId1},
            "userName", "ted");
        t.addExpectation(tagId2, TaggedContentOtherAuthorsView, taggedContentOtherAuthors, new ObjectId[]{tagId2, contentId1, otherAuthorId2},
            "userName", "john");
        t.readView(tenantId, actorId, new ObjectId(TaggedContentOtherAuthorsView, tagId2.getId()), Id.NULL);
        t.assertExpectation(tenantIdAndCentricId);
        t.addExpectation(tagId2, TaggedContentOtherAuthorsView, taggedContentOtherAuthors, new ObjectId[]{tagId2, contentId2, otherAuthorId1},
            "userName", "ted");
        t.addExpectation(tagId2, TaggedContentOtherAuthorsView, taggedContentOtherAuthors, new ObjectId[]{tagId2, contentId2, otherAuthorId2},
            "userName", "john");
        t.readView(tenantId, actorId, new ObjectId(TaggedContentOtherAuthorsView, tagId2.getId()), Id.NULL);
        t.assertExpectation(tenantIdAndCentricId);
        t.clearExpectations();
        System.out.println(mapper.writeValueAsString(view1));
        ObjectNode view2 = t.readView(tenantId, actorId, new ObjectId(TaggedContentOtherAuthorsView, tagId2.getId()), Id.NULL);
        System.out.println(mapper.writeValueAsString(view2));
    }
}
