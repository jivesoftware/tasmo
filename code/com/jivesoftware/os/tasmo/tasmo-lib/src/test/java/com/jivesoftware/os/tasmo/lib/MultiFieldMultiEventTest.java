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
import org.testng.annotations.Test;

/**
 *
 */
public class MultiFieldMultiEventTest extends BaseTasmoTest {

    String ContentView = "ContentView";
    String originalAuthor = "originalAuthor";
    String lastName = "lastName";
    String firstName = "firstName";
    String userName = "userName";

    @Test
    public void testMultiFieldMultiEvent() throws Exception {
        Expectations expectations =
            initModelPaths(ContentView + "::" + originalAuthor + "::Content.ref_originalAuthor.ref.User|User.firstName,lastName,userName");
        ObjectId authorId = write(EventBuilder.create(idProvider, "User", tenantId, actorId).set("firstName", "tom").build());
        //.set("lastName", "sawyer").set("userName", "tsawyer").build());
        ObjectId contentId = write(EventBuilder.create(idProvider, "Content", tenantId, actorId).set("ref_originalAuthor", authorId).build());
        expectations.addExpectation(contentId, ContentView, originalAuthor, new ObjectId[]{contentId, authorId}, firstName, "tom");
        expectations.addExpectation(contentId, ContentView, originalAuthor, new ObjectId[]{contentId, authorId}, lastName, null);
        expectations.addExpectation(contentId, ContentView, originalAuthor, new ObjectId[]{contentId, authorId}, userName, null);
        expectations.assertExpectation(tenantIdAndCentricId);
        expectations.clear();
        authorId = write(EventBuilder.update(authorId, tenantId, actorId).set("lastName", "sawyer").build());
        expectations.addExpectation(contentId, ContentView, originalAuthor, new ObjectId[]{contentId, authorId}, firstName, "tom");
        expectations.addExpectation(contentId, ContentView, originalAuthor, new ObjectId[]{contentId, authorId}, lastName, "sawyer");
        expectations.addExpectation(contentId, ContentView, originalAuthor, new ObjectId[]{contentId, authorId}, userName, null);
        expectations.assertExpectation(tenantIdAndCentricId);
        expectations.clear();
        authorId = write(EventBuilder.update(authorId, tenantId, actorId).set("userName", "tsawyer").build());
        expectations.addExpectation(contentId, ContentView, originalAuthor, new ObjectId[]{contentId, authorId}, firstName, "tom");
        expectations.addExpectation(contentId, ContentView, originalAuthor, new ObjectId[]{contentId, authorId}, lastName, "sawyer");
        expectations.addExpectation(contentId, ContentView, originalAuthor, new ObjectId[]{contentId, authorId}, userName, "tsawyer");
        expectations.assertExpectation(tenantIdAndCentricId);
        expectations.clear();
        ObjectNode view = readView(tenantIdAndCentricId, actorId, new ObjectId(ContentView, contentId.getId()));
        String deserializationInput = mapper.writeValueAsString(view);
        System.out.println("Input:" + deserializationInput);
    }
}
