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
 * @author pete
 */
public class MultiTypedFieldTest extends BaseTasmoTest {

    @Test (invocationCount = 1000, singleThreaded = true, skipFailedInvocations = false)
    public void testMultiTypedFieldsInModelPath() throws Exception {
        String viewClassName = "MultiTypesView";
        String viewFieldName = "pathId";
        Expectations expectations = initModelPaths(viewClassName + "::" + viewFieldName
            + "::CommentVersion.parent.ref.[Document^StatusUpdate]|[Document^StatusUpdate].modDate");
        ObjectId documentId = write(EventBuilder.create(idProvider, "Document", tenantId, actorId).set("modDate", "now").build());
        ObjectId commentVersionId = write(EventBuilder.create(idProvider, "CommentVersion", tenantId, actorId).set("parent", documentId).build());
        ObjectId statusId = write(EventBuilder.create(idProvider, "StatusUpdate", tenantId, actorId).set("modDate", "later").build());
        ObjectId commentId = write(EventBuilder.create(idProvider, "Comment", tenantId, actorId).set("modDate", "eventLater").build());

        expectations.addExpectation(commentVersionId, viewClassName, viewFieldName, new ObjectId[]{ commentVersionId, documentId }, "modDate", "now");
        expectations.assertExpectation(tenantIdAndCentricId);
        expectations.clear();
        ObjectNode view = readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, commentVersionId.getId()));
        System.out.println(mapper.writeValueAsString(view));

        write(EventBuilder.update(commentVersionId, tenantId, actorId).set("parent", statusId).build());
        view = readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, commentVersionId.getId()));
        System.out.println(mapper.writeValueAsString(view));

        expectations.addExpectation(commentVersionId, viewClassName, viewFieldName, new ObjectId[]{ commentVersionId, statusId }, "modDate", "later");
        expectations.assertExpectation(tenantIdAndCentricId);
        expectations.clear();


        write(EventBuilder.update(commentVersionId, tenantId, actorId).set("parent", commentId).build());
        view = readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, commentVersionId.getId()));
        System.out.println(mapper.writeValueAsString(view));

        //setting to a type that was not in the path will blow away the old view and not populate a new one for this path
        expectations.addExpectation(commentVersionId, viewClassName, viewFieldName, new ObjectId[]{ commentVersionId, statusId }, "modDate", null);
        expectations.addExpectation(commentVersionId, viewClassName, viewFieldName, new ObjectId[]{ commentVersionId, documentId }, "modDate", null);
        expectations.addExpectation(commentVersionId, viewClassName, viewFieldName, new ObjectId[]{ commentVersionId, commentId }, "modDate", null);
        expectations.assertExpectation(tenantIdAndCentricId);
        expectations.clear();

        System.out.println("| PASSED |");
    }

    @Test
    public void testMultiTypedFieldsInModelPathWithLatestBackRef() throws Exception {
        String viewClassName = "MultiTypesView";
        String viewFieldName = "pathId";
        Expectations expectations = initModelPaths(viewClassName + "::" + viewFieldName
            + "::CommentVersion.latest_backRef.[Document^StatusUpdate].child|[Document^StatusUpdate].modDate");

        ObjectId commentVersionId = write(EventBuilder.create(idProvider, "CommentVersion", tenantId, actorId).build());
        ObjectId documentId = write(EventBuilder.create(idProvider, "Document", tenantId, actorId).set("modDate", "now")
            .set("child", commentVersionId).build());

        ObjectId statusId = write(EventBuilder.create(idProvider, "StatusUpdate", tenantId, actorId).set("modDate", "later").build());
        ObjectId commentId = write(EventBuilder.create(idProvider, "Comment", tenantId, actorId).set("modDate", "eventLater").build());

        expectations.addExpectation(commentVersionId, viewClassName, viewFieldName, new ObjectId[]{ commentVersionId, documentId }, "modDate", "now");
        expectations.assertExpectation(tenantIdAndCentricId);
        expectations.clear();
        ObjectNode view = readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, commentVersionId.getId()));
        System.out.println(mapper.writeValueAsString(view));

        write(EventBuilder.update(statusId, tenantId, actorId).set("child", commentVersionId).build());

        expectations.addExpectation(commentVersionId, viewClassName, viewFieldName, new ObjectId[]{ commentVersionId, statusId }, "modDate", "later");
        //we don't actually clean up the latest backref anymore
        expectations.addExpectation(commentVersionId, viewClassName, viewFieldName, new ObjectId[]{ commentVersionId, documentId }, "modDate", "now");
        expectations.assertExpectation(tenantIdAndCentricId);
        expectations.clear();
        view = readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, commentVersionId.getId()));
        System.out.println(mapper.writeValueAsString(view));

        write(EventBuilder.update(commentVersionId, tenantId, actorId).set("child", commentVersionId).build());

        //This is correct - but a gotcha. If some type not in the set of types in the path starts referencing something that is, the view will not be updated.
        //In this context, the view means "latest thing I care about to backreference", not "latest thing to back reference"
        expectations.addExpectation(commentVersionId, viewClassName, viewFieldName, new ObjectId[]{ commentVersionId, statusId }, "modDate", "later");
        //we don't actually clean up the latest backref anymore
        expectations.addExpectation(commentVersionId, viewClassName, viewFieldName, new ObjectId[]{ commentVersionId, documentId }, "modDate", "now");
        expectations.addExpectation(commentVersionId, viewClassName, viewFieldName, new ObjectId[]{ commentVersionId, commentId }, "modDate", null);
        expectations.assertExpectation(tenantIdAndCentricId);
        expectations.clear();

        System.out.println("| PASSED |");
    }

    @Test
    public void testMultiTypedFieldsInModelPathWithBackRef() throws Exception {
        String viewClassName = "MultiTypesView";
        String viewFieldName = "pathId";
        Expectations expectations = initModelPaths(viewClassName + "::" + viewFieldName
            + "::CommentVersion.backRefs.[Document^StatusUpdate].child|[Document^StatusUpdate].modDate");

        ObjectId commentVersionId = write(EventBuilder.create(idProvider, "CommentVersion", tenantId, actorId).build());
        ObjectId documentId = write(EventBuilder.create(idProvider, "Document", tenantId, actorId).set("modDate", "now")
            .set("child", commentVersionId).build());

        ObjectId statusId = write(EventBuilder.create(idProvider, "StatusUpdate", tenantId, actorId).set("modDate", "later").build());
        ObjectId commentId = write(EventBuilder.create(idProvider, "Comment", tenantId, actorId).set("modDate", "eventLater").build());

        expectations.addExpectation(commentVersionId, viewClassName, viewFieldName, new ObjectId[]{ commentVersionId, documentId }, "modDate", "now");
        expectations.assertExpectation(tenantIdAndCentricId);
        expectations.clear();
        ObjectNode view = readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, commentVersionId.getId()));
        System.out.println(mapper.writeValueAsString(view));

        write(EventBuilder.update(statusId, tenantId, actorId).set("child", commentVersionId).build());

        expectations.addExpectation(commentVersionId, viewClassName, viewFieldName, new ObjectId[]{ commentVersionId, statusId }, "modDate", "later");
        expectations.addExpectation(commentVersionId, viewClassName, viewFieldName, new ObjectId[]{ commentVersionId, documentId }, "modDate", "now");
        expectations.assertExpectation(tenantIdAndCentricId);
        expectations.clear();
        view = readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, commentVersionId.getId()));
        System.out.println(mapper.writeValueAsString(view));

        write(EventBuilder.update(commentVersionId, tenantId, actorId).set("child", commentVersionId).build());

        expectations.addExpectation(commentVersionId, viewClassName, viewFieldName, new ObjectId[]{ commentVersionId, statusId }, "modDate", "later");
        expectations.addExpectation(commentVersionId, viewClassName, viewFieldName, new ObjectId[]{ commentVersionId, documentId }, "modDate", "now");
        expectations.addExpectation(commentVersionId, viewClassName, viewFieldName, new ObjectId[]{ commentVersionId, commentId }, "modDate", null);
        expectations.assertExpectation(tenantIdAndCentricId);
        expectations.clear();

        System.out.println("| PASSED |");
    }

    @Test (invocationCount = 10, singleThreaded = true, skipFailedInvocations = false)
    public void testCommentListSlugView() throws Exception {
        String viewClassName = "CommentVersionSlugView";
        String searchViewClassName = "CommentSearchView";

        Expectations expectations = initModelPaths(
            viewClassName + "::pathId1::Comment.latest_backRef.CommentVersion.parent|CommentVersion.creationDate,processedBody",
            viewClassName + "::pathId2::Comment.latest_backRef.CommentVersion.parent|CommentVersion.author.ref.User|User.firstName,lastName",
            searchViewClassName + "::pathId3::Comment.parent.ref.[Document^Discussion]|[Document^Discussion].authz");

        ObjectId docId = write(EventBuilder.create(idProvider, "Document", tenantId, actorId).build());
        ObjectId commentId = write(EventBuilder.create(idProvider, "Comment", tenantId, actorId).set("parent", docId).build());

        ObjectId userId = write(EventBuilder.create(idProvider, "User", tenantId, actorId).set("firstName", "ted").set("lastName", "tedson").build());

        ObjectId commentVersionId = write(EventBuilder.create(idProvider, "CommentVersion", tenantId, actorId).set("creationDate", "now")
            .set("processedBody", "booya").set("parent", commentId).set("author", userId).build());

        ObjectNode view = readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, commentId.getId()));
        System.out.println(mapper.writeValueAsString(view));

        expectations.addExpectation(commentId, viewClassName, "pathId1", new ObjectId[]{ commentId, commentVersionId }, "creationDate", "now");
        expectations.addExpectation(commentId, viewClassName, "pathId1", new ObjectId[]{ commentId, commentVersionId }, "processedBody", "booya");

        expectations.addExpectation(commentId, viewClassName, "pathId2", new ObjectId[]{ commentId, commentVersionId, userId }, "firstName", "ted");
        expectations.addExpectation(commentId, viewClassName, "pathId2", new ObjectId[]{ commentId, commentVersionId, userId }, "lastName", "tedson");

        expectations.assertExpectation(tenantIdAndCentricId);
        expectations.clear();

        for (int i = 0; i < 10; i++) {
            commentVersionId = write(EventBuilder.create(idProvider, "CommentVersion", tenantId, actorId).set("creationDate", "later" + i)
                .set("processedBody", "awwyeah").set("parent", commentId).set("author", userId).build());

            view = readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, commentId.getId()));
            System.out.println(mapper.writeValueAsString(view));

            expectations.addExpectation(commentId, viewClassName, "pathId1", new ObjectId[]{ commentId, commentVersionId }, "creationDate", "later" + i);
            expectations.addExpectation(commentId, viewClassName, "pathId1", new ObjectId[]{ commentId, commentVersionId }, "processedBody", "awwyeah");

            expectations.addExpectation(commentId, viewClassName, "pathId2", new ObjectId[]{ commentId, commentVersionId, userId }, "firstName", "ted");
            expectations.addExpectation(commentId, viewClassName, "pathId2", new ObjectId[]{ commentId, commentVersionId, userId }, "lastName", "tedson");

            expectations.assertExpectation(tenantIdAndCentricId);
            expectations.clear();

            write(EventBuilder.update(docId, tenantId, actorId).set("authz", "somedamnthing").build());

            view = readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, commentId.getId()));
            System.out.println(mapper.writeValueAsString(view));

            expectations.addExpectation(commentId, viewClassName, "pathId1", new ObjectId[]{ commentId, commentVersionId }, "creationDate", "later" + i);
            expectations.addExpectation(commentId, viewClassName, "pathId1", new ObjectId[]{ commentId, commentVersionId }, "processedBody", "awwyeah");

            expectations.addExpectation(commentId, viewClassName, "pathId2", new ObjectId[]{ commentId, commentVersionId, userId }, "firstName", "ted");
            expectations.addExpectation(commentId, viewClassName, "pathId2", new ObjectId[]{ commentId, commentVersionId, userId }, "lastName", "tedson");

            expectations.assertExpectation(tenantIdAndCentricId);
            expectations.clear();
        }

        System.out.println("| PASSED |");
    }
}
