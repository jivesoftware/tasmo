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
import org.testng.annotations.Test;

/**
 *
 */
public class BackRefBridgeTest extends BaseTasmoTest {

    @Test(invocationCount = 10, singleThreaded = true, enabled = true)
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


        events.add(commentVersion);
        events.add(documentVersionEvent);
        events.add(commentEvent);
        events.add(documentEvent);
        events.add(userEvent);

        write(events);

        ObjectNode view = readView(tenantIdAndCentricId, actorId, new ObjectId(commentClass, comment1.getId()));
        System.out.println(mapper.writeValueAsString(view));

        expectations.addExpectation(comment1, commentClass, content, new ObjectId[]{comment1, doc1, documentVersion1}, "processedSubject", "subject");
        expectations.addExpectation(comment1, commentClass, root, new ObjectId[]{comment1, commentVersion1, user1}, "userName", "ted");
        expectations.assertExpectation(tenantIdAndCentricId);
        expectations.clear();

        System.out.println("------------------------------------------");

    }

    ObjectId nextId(String className) {
        return new ObjectId(className, idProvider.nextId());
    }
}
