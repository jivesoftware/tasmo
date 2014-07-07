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
import org.testng.annotations.Test;

/**
 *
 */
public class MultiFieldMultiViewTest extends BaseTest {

    String ContentView = "ContentView";
    String ContainerView = "ContainerView";
    String originalAuthorName = "originalAuthorName";
    String originalAuthor = "originalAuthor";
    String lastName = "lastName";
    String firstName = "firstName";
    String userName = "userName";

    @Test (dataProvider = "tasmoMaterializer", invocationCount = 1, singleThreaded = true)
    public void testMultiFieldMultiView(TasmoMaterializerHarness t) throws Exception {
        String contentView1 = ContentView + "1";
        String contentView2 = ContentView + "2";
        Views views = TasmoModelFactory.modelToViews(contentView1 + "::" + originalAuthor + "::Content.ref_originalAuthor.ref.User|User.firstName,lastName,userName", contentView2 + "::"
            + originalAuthor + "::Content.ref_originalAuthor.ref.User|User.firstName,lastName,userName");
        t.initModel(views);

        ObjectId authorId =
            t.write(EventBuilder.create(t.idProvider(), "User", tenantId, actorId).set("firstName", "tom").set("lastName", "sawyer")
            .set("userName", "tsawyer").build());
        ObjectId contentId = t.write(EventBuilder.create(t.idProvider(), "Content", tenantId, actorId).set("ref_originalAuthor", authorId).build());
        t.addExpectation(contentId, contentView1, originalAuthor, new ObjectId[]{contentId, authorId}, firstName, "tom");
        t.addExpectation(contentId, contentView1, originalAuthor, new ObjectId[]{contentId, authorId}, lastName, "sawyer");
        t.addExpectation(contentId, contentView1, originalAuthor, new ObjectId[]{contentId, authorId}, userName, "tsawyer");
        t.addExpectation(contentId, contentView2, originalAuthor, new ObjectId[]{contentId, authorId}, firstName, "tom");
        t.addExpectation(contentId, contentView2, originalAuthor, new ObjectId[]{contentId, authorId}, lastName, "sawyer");
        t.addExpectation(contentId, contentView2, originalAuthor, new ObjectId[]{contentId, authorId}, userName, "tsawyer");
        t.readView(tenantIdAndCentricId, actorId, new ObjectId(contentView1, contentId.getId()));
        t.readView(tenantIdAndCentricId, actorId, new ObjectId(contentView2, contentId.getId()));
        t.assertExpectation(tenantIdAndCentricId);
        t.clearExpectations();
        ObjectNode view1 = t.readView(tenantIdAndCentricId, actorId, new ObjectId(contentView1, contentId.getId()));
        String deserializationInput1 = mapper.writeValueAsString(view1);
        System.out.println("Input1:" + deserializationInput1);
        ObjectNode view2 = t.readView(tenantIdAndCentricId, actorId, new ObjectId(contentView2, contentId.getId()));
        String deserializationInput2 = mapper.writeValueAsString(view2);
        System.out.println("Input2:" + deserializationInput2);
    }
}
