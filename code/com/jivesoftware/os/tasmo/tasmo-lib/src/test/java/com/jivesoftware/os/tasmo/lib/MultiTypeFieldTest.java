/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.jivesoftware.os.tasmo.lib;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.jivesoftware.os.jive.utils.id.ObjectId;
import com.jivesoftware.os.tasmo.event.api.write.EventBuilder;
import com.jivesoftware.os.tasmo.model.Views;
import org.testng.annotations.Test;

/**
 *
 * @author pete
 */
public class MultiTypeFieldTest extends BaseTest {

    String ContentView = "ContentView";
    String originalAuthorName = "originalAuthorName";
    String originalAuthor = "originalAuthor";
    String lastName = "lastName";
    String firstName = "firstName";
    String userName = "userName";

    @Test (dataProvider = "tasmoMaterializer", invocationCount = 1, singleThreaded = true)
    public void testMultiField(TasmoMaterializerHarness t) throws Exception {
        Views views = TasmoModelFactory.modelToViews(ContentView + "::" + originalAuthor
                + "::[Document^Thread].ref_originalAuthor.ref.[User^Anonymous]|[User^Anonymous].firstName,lastName,userName");
        t.initModel(views);

        ObjectId authorId =
            t.write(EventBuilder.create(t.idProvider(), "User", tenantId, actorId).set("firstName", "tom").set("lastName", "sawyer")
            .set("userName", "tsawyer").build());
        ObjectId contentId = t.write(EventBuilder.create(t.idProvider(), "Thread", tenantId, actorId).set("ref_originalAuthor", authorId).build());
        t.addExpectation(contentId, ContentView, originalAuthor, new ObjectId[]{contentId, authorId}, firstName, "tom");
        t.addExpectation(contentId, ContentView, originalAuthor, new ObjectId[]{contentId, authorId}, lastName, "sawyer");
        t.addExpectation(contentId, ContentView, originalAuthor, new ObjectId[]{contentId, authorId}, userName, "tsawyer");
        t.assertExpectation(tenantIdAndCentricId);
        t.clearExpectations();
        ObjectNode view = t.readView(tenantIdAndCentricId, actorId, new ObjectId(ContentView, contentId.getId()));
        String deserializationInput = mapper.writeValueAsString(view);
        System.out.println("Input:" + deserializationInput);


        authorId = t.write(EventBuilder.create(t.idProvider(), "Anonymous", tenantId, actorId).set("firstName", "tom").set("lastName", "sawyer")
            .set("userName", "tsawyer").build());
        contentId = t.write(EventBuilder.create(t.idProvider(), "Document", tenantId, actorId).set("ref_originalAuthor", authorId).build());
        t.addExpectation(contentId, ContentView, originalAuthor, new ObjectId[]{contentId, authorId}, firstName, "tom");
        t.addExpectation(contentId, ContentView, originalAuthor, new ObjectId[]{contentId, authorId}, lastName, "sawyer");
        t.addExpectation(contentId, ContentView, originalAuthor, new ObjectId[]{contentId, authorId}, userName, "tsawyer");
        t.assertExpectation(tenantIdAndCentricId);
        t.clearExpectations();
        view = t.readView(tenantIdAndCentricId, actorId, new ObjectId(ContentView, contentId.getId()));
        deserializationInput = mapper.writeValueAsString(view);
        System.out.println("Input:" + deserializationInput);
    }
}
