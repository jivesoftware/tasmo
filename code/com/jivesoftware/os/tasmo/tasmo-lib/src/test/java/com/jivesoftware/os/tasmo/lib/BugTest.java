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
import com.jivesoftware.os.tasmo.event.api.write.Event;
import com.jivesoftware.os.tasmo.event.api.write.EventBuilder;
import com.jivesoftware.os.tasmo.id.ObjectId;
import java.util.ArrayList;
import java.util.List;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.testng.annotations.Test;

/**
 *
 */
public class BugTest extends BaseTasmoTest {
    /*

     View: alsfp6wsq2rnq
     {
     "objectId" : "Comment_alsfp6wsq2rnq",
     "content" : {
     "objectId" : "Document_alsfp6wsq2rkm",
     "latest_versionParent" : {
     "objectId" : "DocumentVersion_alsfp6wsq2rm4",
     "processedSubject" : "API Test Document M4n86pc7jmJbup12I1M9yQdtHdoVyGpN"
     }
     },
     "latest_root" : {
     "objectId" : "CommentVersion_alsfp6wsq2roe",
     "author" : {
     "objectId" : "User_alsfbncsy2biq",
     "username" : "testuserjszwwhuj.lsbbncfbcqhg"
     }
     },
     "viewClassName" : "CommentAll2View",
     "tenantId" : "api-tests-npbfqo"
     }

     Expectations expectations = initModelPaths(
     viewClassName + "::pathId1::Comment.latest_backRef.CommentVersion.parent|CommentVersion.creationDate,processedBody",
     viewClassName + "::pathId2::Comment.latest_backRef.CommentVersion.parent|CommentVersion.author.ref.User|User.firstName,lastName",
     searchViewClassName + "::pathId3::Comment.parent.ref.[Document^Discussion]|[Document^Discussion].authz");




     {[[Comment].pid.content.ref.Document,
     [Document, StatusUpdate, Post, Discussion, File, BlogPost].latest_backRef.
     [PostVersion, BlogPostVersion, CommentVersion, FileVersion, DiscussionVersion, DocumentVersion, StatusUpdateVersion].id.versionParent,
     [PostVersion, BlogPostVersion, CommentVersion, FileVersion, DiscussionVersion, DocumentVersion, StatusUpdateVersion].id.processedSubject]
     }

     {[[Comment].latest_backRef.[CommentVersion].pid.root, [CommentVersion].id.creationDate]}
     {[[Comment].latest_backRef.[CommentVersion].pid.root, [CommentVersion].id.author.ref.[User], [User].id.username]}


     MODELPATH ModelPath{pathMembers=[[Comment].pid.content.ref.[Document, StatusUpdate, Post, Discussion, File, BlogPost],
    [Document, StatusUpdate, Post, Discussion, File, BlogPost].latest_backRef.[PostVersion, BlogPostVersion, CommentVersion,
    FileVersion, DiscussionVersion, DocumentVersion, StatusUpdateVersion].id.versionParent, [PostVersion, BlogPostVersion,
    CommentVersion, FileVersion, DiscussionVersion, DocumentVersion, StatusUpdateVersion].id.processedSubject]}
     MODELPATH ModelPath{pathMembers=[[Comment].latest_backRef.[CommentVersion].pid.root, [CommentVersion].id.author.ref.
    [User], [User].id.username]}

     */

    @Test (invocationCount = 10, singleThreaded = true, enabled = true)
    public void testRef() throws Exception {

//        LogManager.getLogger("com.jivesoftware.os.tasmo").setLevel(Level.TRACE);
//        LogManager.getLogger("com.jivesoftware.os.tasmo.lib.concur.ConcurrencyAndExistanceCommitChange").setLevel(Level.TRACE);
//        LogManager.getLogger("com.jivesoftware.os.tasmo.reference.lib.ReferenceStore").setLevel(Level.TRACE);
//        LogManager.getLogger("com.jivesoftware.os.tasmo.view.reader.service.writer.WriteToViewValueStore").setLevel(Level.TRACE);


        System.out.println("+++++++++++++++++++++++++++++++++++++++");

        String commentClass = "Comment";
        String content = "content";
        String root = "root";
        Expectations expectations = initModelPaths(
            commentClass + "::" + content + "::Comment.content.ref.Document|"
            + "Document.latest_backRef.[CommentVersion^DocumentVersion].versionParent|"
            + "[CommentVersion^DocumentVersion].processedSubject",
            commentClass + "::" + root + "::Comment.latest_backRef.CommentVersion.root|CommentVersion.author.ref.User|User.userName"
        );

        // Content -> Document <- CommentVersion.processedSubject
        // Leaf-Content.content -> Document | Leaf
        // Document versionParent

        List<Event> events = new ArrayList<>();

        ObjectId user1 = nextId("User");
        Event userEvent = EventBuilder.create(user1, tenantId, actorId)
            .set("userName", "ted").build();

        ObjectId doc1 = nextId("Document");
        Event documentEvent = EventBuilder.create(doc1, tenantId, actorId)
            .set("name", "doc1").build();

        ObjectId documentVersion1 = nextId("DocumentVersion");
        Event documentVersionEvent = EventBuilder.create(documentVersion1, tenantId, actorId)
            .set("processedSubject", "subject")
            .set("versionParent", doc1).build();

        ObjectId comment1 = nextId("Comment");
        Event commentEvent = EventBuilder.create(comment1, tenantId, actorId)
            .set("content", doc1)
            .set("name", "comment1").build();

        ObjectId commentVersion1 = nextId("CommentVersion");
        Event commentVersion = EventBuilder.create(commentVersion1, tenantId, actorId)
            .set("author", user1)
            .set("root", comment1).build();

        // PASS
//        events.add(documentEvent);
//        events.add(commentEvent);
//        events.add(userEvent);
//        events.add(documentVersionEvent);
//        events.add(commentVersion);


        // FAIL
        events.add(commentVersion);
        events.add(documentVersionEvent);
        events.add(commentEvent);
        events.add(documentEvent);
        events.add(userEvent);


        write(events);

        ObjectNode view = readView(tenantIdAndCentricId, actorId, new ObjectId(commentClass, comment1.getId()));
        System.out.println(mapper.writeValueAsString(view));

        expectations.addExpectation(comment1, commentClass, content, new ObjectId[]{ comment1, doc1, documentVersion1 }, "processedSubject", "subject");
        expectations.addExpectation(comment1, commentClass, root, new ObjectId[]{ comment1, commentVersion1, user1 }, "userName", "ted");
        expectations.assertExpectation(tenantIdAndCentricId);
        expectations.clear();

        System.out.println("------------------------------------------");

    }

    ObjectId nextId(String className) {
        return new ObjectId(className, idProvider.nextId());
    }
}
