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
 * @author pete
 */
public class MultiTypedFieldTest extends BaseTest {

    @Test (dataProvider = "tasmoMaterializer", invocationCount = 1, singleThreaded = true, skipFailedInvocations = false)
    public void testMultiTypedFieldsInModelPath(TasmoMaterializerHarness t) throws Exception {
        String viewClassName = "MultiTypesView";
        String viewFieldName = "pathId";
        Views views = TasmoModelFactory.modelToViews(viewClassName + "::" + viewFieldName
            + "::CommentVersion.parent.ref.[Document^StatusUpdate]|[Document^StatusUpdate].modDate");
        t.initModel(views);

        ObjectId documentId = t.write(EventBuilder.create(t.idProvider(), "Document", tenantId, actorId).set("modDate", "now").build());
        ObjectId commentVersionId = t.write(EventBuilder.create(t.idProvider(), "CommentVersion", tenantId, actorId).set("parent", documentId).build());
        ObjectId statusId = t.write(EventBuilder.create(t.idProvider(), "StatusUpdate", tenantId, actorId).set("modDate", "later").build());
        ObjectId commentId = t.write(EventBuilder.create(t.idProvider(), "Comment", tenantId, actorId).set("modDate", "eventLater").build());

        t.addExpectation(commentVersionId, viewClassName, viewFieldName, new ObjectId[]{ commentVersionId, documentId }, "modDate", "now");
        t.readView(tenantId, actorId, new ObjectId(viewClassName, commentVersionId.getId()), Id.NULL);
        t.assertExpectation(tenantIdAndCentricId);
        t.clearExpectations();
        ObjectNode view = t.readView(tenantId, actorId, new ObjectId(viewClassName, commentVersionId.getId()), Id.NULL);
        System.out.println(mapper.writeValueAsString(view));

        t.write(EventBuilder.update(commentVersionId, tenantId, actorId).set("parent", statusId).build());
        view = t.readView(tenantId, actorId, new ObjectId(viewClassName, commentVersionId.getId()), Id.NULL);
        System.out.println(mapper.writeValueAsString(view));

        t.addExpectation(commentVersionId, viewClassName, viewFieldName, new ObjectId[]{ commentVersionId, statusId }, "modDate", "later");
        t.readView(tenantId, actorId, new ObjectId(viewClassName, commentVersionId.getId()), Id.NULL);
        t.assertExpectation(tenantIdAndCentricId);
        t.clearExpectations();


        t.write(EventBuilder.update(commentVersionId, tenantId, actorId).set("parent", commentId).build());
        view = t.readView(tenantId, actorId, new ObjectId(viewClassName, commentVersionId.getId()), Id.NULL);
        System.out.println(mapper.writeValueAsString(view));

        //setting to a type that was not in the path will blow away the old view and not populate a new one for this path
        t.addExpectation(commentVersionId, viewClassName, viewFieldName, new ObjectId[]{ commentVersionId, statusId }, "modDate", null);
        t.addExpectation(commentVersionId, viewClassName, viewFieldName, new ObjectId[]{ commentVersionId, documentId }, "modDate", null);
        t.addExpectation(commentVersionId, viewClassName, viewFieldName, new ObjectId[]{ commentVersionId, commentId }, "modDate", null);
        t.readView(tenantId, actorId, new ObjectId(viewClassName, commentVersionId.getId()), Id.NULL);
        t.assertExpectation(tenantIdAndCentricId);
        t.clearExpectations();

        System.out.println("| PASSED |");
    }

    @Test (dataProvider = "tasmoMaterializer", invocationCount = 1, singleThreaded = true)
    public void testMultiTypedFieldsInModelPathWithLatestBackRef(TasmoMaterializerHarness t) throws Exception {
        String viewClassName = "MultiTypesView";
        String viewFieldName = "pathId";
        Views views = TasmoModelFactory.modelToViews(viewClassName + "::" + viewFieldName
            + "::CommentVersion.latest_backRef.[Document^StatusUpdate].child|[Document^StatusUpdate].modDate");
        t.initModel(views);

        ObjectId commentVersionId = t.write(EventBuilder.create(t.idProvider(), "CommentVersion", tenantId, actorId).build());
        ObjectId documentId = t.write(EventBuilder.create(t.idProvider(), "Document", tenantId, actorId).set("modDate", "now")
            .set("child", commentVersionId).build());

        ObjectId statusId = t.write(EventBuilder.create(t.idProvider(), "StatusUpdate", tenantId, actorId).set("modDate", "later").build());
        ObjectId commentId = t.write(EventBuilder.create(t.idProvider(), "Comment", tenantId, actorId).set("modDate", "eventLater").build());

        t.addExpectation(commentVersionId, viewClassName, viewFieldName, new ObjectId[]{ commentVersionId, documentId }, "modDate", "now");
        t.readView(tenantId, actorId, new ObjectId(viewClassName, commentVersionId.getId()), Id.NULL);
        t.assertExpectation(tenantIdAndCentricId);
        t.clearExpectations();
        ObjectNode view = t.readView(tenantId, actorId, new ObjectId(viewClassName, commentVersionId.getId()), Id.NULL);
        System.out.println(mapper.writeValueAsString(view));

        t.write(EventBuilder.update(statusId, tenantId, actorId).set("child", commentVersionId).build());

        t.addExpectation(commentVersionId, viewClassName, viewFieldName, new ObjectId[]{ commentVersionId, statusId }, "modDate", "later");
        //we don't actually clean up the latest backref anymore
        t.addExpectation(commentVersionId, viewClassName, viewFieldName, new ObjectId[]{ commentVersionId, documentId }, "modDate", "now");
        t.readView(tenantId, actorId, new ObjectId(viewClassName, commentVersionId.getId()), Id.NULL);
        t.assertExpectation(tenantIdAndCentricId);
        t.clearExpectations();
        view = t.readView(tenantId, actorId, new ObjectId(viewClassName, commentVersionId.getId()), Id.NULL);
        System.out.println(mapper.writeValueAsString(view));

        t.write(EventBuilder.update(commentVersionId, tenantId, actorId).set("child", commentVersionId).build());

        //This is correct - but a gotcha. If some type not in the set of types in the path starts referencing something that is, the view will not be updated.
        //In this context, the view means "latest thing I care about to backreference", not "latest thing to back reference"
        t.addExpectation(commentVersionId, viewClassName, viewFieldName, new ObjectId[]{ commentVersionId, statusId }, "modDate", "later");
        //we don't actually clean up the latest backref anymore
        t.addExpectation(commentVersionId, viewClassName, viewFieldName, new ObjectId[]{ commentVersionId, documentId }, "modDate", "now");
        t.addExpectation(commentVersionId, viewClassName, viewFieldName, new ObjectId[]{ commentVersionId, commentId }, "modDate", null);
        t.readView(tenantId, actorId, new ObjectId(viewClassName, commentVersionId.getId()), Id.NULL);
        t.assertExpectation(tenantIdAndCentricId);
        t.clearExpectations();

        System.out.println("| PASSED |");
    }

    @Test (dataProvider = "tasmoMaterializer", invocationCount = 1, singleThreaded = true)
    public void testMultiTypedFieldsInModelPathWithBackRef(TasmoMaterializerHarness t) throws Exception {
        String viewClassName = "MultiTypesView";
        String viewFieldName = "pathId";
        Views views = TasmoModelFactory.modelToViews(viewClassName + "::" + viewFieldName
            + "::CommentVersion.backRefs.[Document^StatusUpdate].child|[Document^StatusUpdate].modDate");
        t.initModel(views);

        ObjectId commentVersionId = t.write(EventBuilder.create(t.idProvider(), "CommentVersion", tenantId, actorId).build());
        ObjectId documentId = t.write(EventBuilder.create(t.idProvider(), "Document", tenantId, actorId).set("modDate", "now")
            .set("child", commentVersionId).build());

        ObjectId statusId = t.write(EventBuilder.create(t.idProvider(), "StatusUpdate", tenantId, actorId).set("modDate", "later").build());
        ObjectId commentId = t.write(EventBuilder.create(t.idProvider(), "Comment", tenantId, actorId).set("modDate", "eventLater").build());

        t.addExpectation(commentVersionId, viewClassName, viewFieldName, new ObjectId[]{ commentVersionId, documentId }, "modDate", "now");
        t.readView(tenantId, actorId, new ObjectId(viewClassName, commentVersionId.getId()), Id.NULL);
        t.assertExpectation(tenantIdAndCentricId);
        t.clearExpectations();
        ObjectNode view = t.readView(tenantId, actorId, new ObjectId(viewClassName, commentVersionId.getId()), Id.NULL);
        System.out.println(mapper.writeValueAsString(view));

        t.write(EventBuilder.update(statusId, tenantId, actorId).set("child", commentVersionId).build());

        t.addExpectation(commentVersionId, viewClassName, viewFieldName, new ObjectId[]{ commentVersionId, statusId }, "modDate", "later");
        t.addExpectation(commentVersionId, viewClassName, viewFieldName, new ObjectId[]{ commentVersionId, documentId }, "modDate", "now");
        t.readView(tenantId, actorId, new ObjectId(viewClassName, commentVersionId.getId()), Id.NULL);
        t.assertExpectation(tenantIdAndCentricId);
        t.clearExpectations();
        view = t.readView(tenantId, actorId, new ObjectId(viewClassName, commentVersionId.getId()), Id.NULL);
        System.out.println(mapper.writeValueAsString(view));

        t.write(EventBuilder.update(commentVersionId, tenantId, actorId).set("child", commentVersionId).build());

        t.addExpectation(commentVersionId, viewClassName, viewFieldName, new ObjectId[]{ commentVersionId, statusId }, "modDate", "later");
        t.addExpectation(commentVersionId, viewClassName, viewFieldName, new ObjectId[]{ commentVersionId, documentId }, "modDate", "now");
        t.addExpectation(commentVersionId, viewClassName, viewFieldName, new ObjectId[]{ commentVersionId, commentId }, "modDate", null);
        t.readView(tenantId, actorId, new ObjectId(viewClassName, commentVersionId.getId()), Id.NULL);
        t.assertExpectation(tenantIdAndCentricId);
        t.clearExpectations();

        System.out.println("| PASSED |");
    }

    @Test (dataProvider = "tasmoMaterializer", invocationCount = 10, singleThreaded = true, skipFailedInvocations = false)
    public void testCommentListSlugView(TasmoMaterializerHarness t) throws Exception {
        String viewClassName = "CommentVersionSlugView";
        String searchViewClassName = "CommentSearchView";

        Views views = TasmoModelFactory.modelToViews(
            viewClassName + "::pathId1::Comment.latest_backRef.CommentVersion.parent|CommentVersion.creationDate,processedBody",
            viewClassName + "::pathId2::Comment.latest_backRef.CommentVersion.parent|CommentVersion.author.ref.User|User.firstName,lastName",
            searchViewClassName + "::pathId3::Comment.parent.ref.[Document^Discussion]|[Document^Discussion].authz");
        t.initModel(views);

        ObjectId docId = t.write(EventBuilder.create(t.idProvider(), "Document", tenantId, actorId).build());
        ObjectId commentId = t.write(EventBuilder.create(t.idProvider(), "Comment", tenantId, actorId).set("parent", docId).build());

        ObjectId userId = t.write(EventBuilder.create(t.idProvider(), "User", tenantId, actorId).set("firstName", "ted").set("lastName", "tedson").build());

        ObjectId commentVersionId = t.write(EventBuilder.create(t.idProvider(), "CommentVersion", tenantId, actorId).set("creationDate", "now")
            .set("processedBody", "booya").set("parent", commentId).set("author", userId).build());

        ObjectNode view = t.readView(tenantId, actorId, new ObjectId(viewClassName, commentId.getId()), Id.NULL);
        System.out.println(mapper.writeValueAsString(view));

        t.addExpectation(commentId, viewClassName, "pathId1", new ObjectId[]{ commentId, commentVersionId }, "creationDate", "now");
        t.addExpectation(commentId, viewClassName, "pathId1", new ObjectId[]{ commentId, commentVersionId }, "processedBody", "booya");

        t.addExpectation(commentId, viewClassName, "pathId2", new ObjectId[]{ commentId, commentVersionId, userId }, "firstName", "ted");
        t.addExpectation(commentId, viewClassName, "pathId2", new ObjectId[]{ commentId, commentVersionId, userId }, "lastName", "tedson");

        t.readView(tenantId, actorId, new ObjectId(viewClassName, commentId.getId()), Id.NULL);
        t.assertExpectation(tenantIdAndCentricId);
        t.clearExpectations();

        for (int i = 0; i < 10; i++) {
            commentVersionId = t.write(EventBuilder.create(t.idProvider(), "CommentVersion", tenantId, actorId).set("creationDate", "later" + i)
                .set("processedBody", "awwyeah").set("parent", commentId).set("author", userId).build());

            view = t.readView(tenantId, actorId, new ObjectId(viewClassName, commentId.getId()), Id.NULL);
            System.out.println(mapper.writeValueAsString(view));

            t.addExpectation(commentId, viewClassName, "pathId1", new ObjectId[]{ commentId, commentVersionId }, "creationDate", "later" + i);
            t.addExpectation(commentId, viewClassName, "pathId1", new ObjectId[]{ commentId, commentVersionId }, "processedBody", "awwyeah");

            t.addExpectation(commentId, viewClassName, "pathId2", new ObjectId[]{ commentId, commentVersionId, userId }, "firstName", "ted");
            t.addExpectation(commentId, viewClassName, "pathId2", new ObjectId[]{ commentId, commentVersionId, userId }, "lastName", "tedson");

            t.assertExpectation(tenantIdAndCentricId);
            t.clearExpectations();

            t.write(EventBuilder.update(docId, tenantId, actorId).set("authz", "somedamnthing").build());

            view = t.readView(tenantId, actorId, new ObjectId(viewClassName, commentId.getId()), Id.NULL);
            System.out.println(mapper.writeValueAsString(view));

            t.addExpectation(commentId, viewClassName, "pathId1", new ObjectId[]{ commentId, commentVersionId }, "creationDate", "later" + i);
            t.addExpectation(commentId, viewClassName, "pathId1", new ObjectId[]{ commentId, commentVersionId }, "processedBody", "awwyeah");

            t.addExpectation(commentId, viewClassName, "pathId2", new ObjectId[]{ commentId, commentVersionId, userId }, "firstName", "ted");
            t.addExpectation(commentId, viewClassName, "pathId2", new ObjectId[]{ commentId, commentVersionId, userId }, "lastName", "tedson");

            t.assertExpectation(tenantIdAndCentricId);
            t.clearExpectations();
        }

        System.out.println("| PASSED |");
    }
}
