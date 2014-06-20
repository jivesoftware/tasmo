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
import org.testng.annotations.Test;

/**
 *
 */
public class MultiFieldDeletesMultiEventTest extends BaseTasmoTest {

    String ContentView = "ContentView";
    String originalAuthor = "originalAuthor";
    String lastName = "lastName";
    String firstName = "firstName";
    String userName = "userName";

    @Test
    public void testMultiFieldDeletesMultiEvent() throws Exception {
        Expectations expectations = initModelPaths(
                ContentView + "::" + originalAuthor + "::Content.ref_originalAuthor.ref.User|User.firstName,lastName,userName");
        ObjectId authorId = write(EventBuilder.create(idProvider, "User", tenantId, actorId).set("firstName", "tom").set("lastName", "sawyer")
                .set("userName", "tsawyer")
                .build());
        ObjectId contentId = write(EventBuilder.create(idProvider, "Content", tenantId, actorId).set("ref_originalAuthor", authorId).build());
        System.out.println("--------------------------------------------------------------------------");

        expectations.addExpectation(contentId, ContentView, originalAuthor, new ObjectId[]{contentId, authorId}, firstName, "tom");
        expectations.addExpectation(contentId, ContentView, originalAuthor, new ObjectId[]{contentId, authorId}, lastName, "sawyer");
        expectations.addExpectation(contentId, ContentView, originalAuthor, new ObjectId[]{contentId, authorId}, userName, "tsawyer");
        expectations.assertExpectation(tenantIdAndCentricId);
        expectations.clear();

        authorId = write(EventBuilder.update(authorId, tenantId, actorId).clear("userName").build());
        System.out.println("--------------------------------------------------------------------------");

        expectations.addExpectation(contentId, ContentView, originalAuthor, new ObjectId[]{contentId, authorId}, firstName, "tom");
        expectations.addExpectation(contentId, ContentView, originalAuthor, new ObjectId[]{contentId, authorId}, lastName, "sawyer");
        expectations.addExpectation(contentId, ContentView, originalAuthor, new ObjectId[]{contentId, authorId}, userName, null);
        expectations.assertExpectation(tenantIdAndCentricId);
        expectations.clear();

        authorId = write(EventBuilder.update(authorId, tenantId, actorId).clear("lastName").build());
        System.out.println("--------------------------------------------------------------------------");

        expectations.addExpectation(contentId, ContentView, originalAuthor, new ObjectId[]{contentId, authorId}, firstName, "tom");
        expectations.addExpectation(contentId, ContentView, originalAuthor, new ObjectId[]{contentId, authorId}, lastName, null);
        expectations.addExpectation(contentId, ContentView, originalAuthor, new ObjectId[]{contentId, authorId}, userName, null);
        expectations.assertExpectation(tenantIdAndCentricId);
        expectations.clear();

        authorId = write(EventBuilder.update(authorId, tenantId, actorId).clear("firstName").build());
        System.out.println("--------------------------------------------------------------------------");

        ObjectNode view = readView(tenantIdAndCentricId, actorId, new ObjectId(ContentView, contentId.getId()));
        System.out.println("View:" + view);

        expectations.addExpectation(contentId, ContentView, originalAuthor, new ObjectId[]{contentId, authorId}, firstName, null);
        expectations.addExpectation(contentId, ContentView, originalAuthor, new ObjectId[]{contentId, authorId}, lastName, null);
        expectations.addExpectation(contentId, ContentView, originalAuthor, new ObjectId[]{contentId, authorId}, userName, null);
        expectations.assertExpectation(tenantIdAndCentricId);
        expectations.clear();


    }
}
