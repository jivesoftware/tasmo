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
import org.testng.annotations.Test;

/**
 *
 */
public class MultiFieldWithRefsTest extends BaseTest {

    String ContentView = "ContentView";
    String originalAuthor = "originalAuthor";
    String lastName = "lastName";
    String firstName = "firstName";
    String userName = "userName";

    @Test (dataProvider = "tasmoMaterializer", invocationCount = 1, singleThreaded = true)
    public void testMultiFieldWithRefs(TasmoMaterializerHarness t) throws Exception {
        Views views = TasmoModelFactory.modelToViews(
            ContentView + "::" + originalAuthor + "::Content.ref_originalAuthor.ref.User|User.firstName,lastName,userName,ref_address");
        t.initModel(views);

        ObjectId authorId =
            t.write(EventBuilder.create(t.idProvider(), "User", tenantId, actorId).set("firstName", "tom").set("lastName", "sawyer")
                .set("userName", "tsawyer").set("ref_address", new ObjectId("Address", new Id(9_876))).build());
        ObjectId contentId = t.write(EventBuilder.create(t.idProvider(), "Content", tenantId, actorId).set("ref_originalAuthor", authorId).build());
        t.addExpectation(contentId, ContentView, originalAuthor, new ObjectId[]{ contentId, authorId }, firstName, "tom");
        t.addExpectation(contentId, ContentView, originalAuthor, new ObjectId[]{ contentId, authorId }, lastName, "sawyer");
        t.addExpectation(contentId, ContentView, originalAuthor, new ObjectId[]{ contentId, authorId }, userName, "tsawyer");
        t.readView(tenantIdAndCentricId, actorId, new ObjectId(ContentView, contentId.getId()));
        t.assertExpectation(tenantIdAndCentricId);
        t.clearExpectations();
        ObjectNode view = t.readView(tenantIdAndCentricId, actorId, new ObjectId(ContentView, contentId.getId()));
        String deserializationInput = mapper.writeValueAsString(view);
        System.out.println("Input:" + deserializationInput);
    }
}
