/*
 * $Revision$
 * $Date$
 *
 * Copyright (C) 1999-$year$ Jive Software. All rights reserved.
 *
 * This software is the proprietary information of Jive Software. Use is subject to license terms.
 */
package com.jivesoftware.os.tasmo.lib;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.jivesoftware.os.jive.utils.id.ObjectId;
import com.jivesoftware.os.tasmo.event.api.ReservedFields;
import com.jivesoftware.os.tasmo.event.api.write.EventBuilder;
import java.util.Arrays;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 *
 */
public class DeletesTest extends BaseTasmoTest {

    @Test
    public void testDeletes() throws Exception {
        String viewClassName = "Values";
        String viewFieldName = "userInfo";
        Expectations expectations = initModelPaths(viewClassName + "::" + viewFieldName + "::User.userName,age");
        ObjectId user1 = write(EventBuilder.create(idProvider, "User", tenantId, actorId).set("userName", "ted").build());

        expectations.addExpectation(user1, viewClassName, viewFieldName, new ObjectId[]{user1}, "userName", "ted");
        expectations.assertExpectation(tenantIdAndCentricId);
        expectations.clear();

        ObjectNode view = readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, user1.getId()));
        Assert.assertNotNull(view);

        // - 3
        write(EventBuilder.update(user1, tenantId, actorId).set(ReservedFields.DELETED, true).build());

        view = readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, user1.getId()));
        Assert.assertNull(view);

    }

    @Test
    public void testDeleteReferenced() throws Exception {
        String viewClassName = "Values";
        String viewFieldName = "userInfo";
        Expectations expectations = initModelPaths(viewClassName + "::" + viewFieldName + "::Content.ref_originalAuthor.ref.User|User.userName");

        ObjectId user1 = write(EventBuilder.create(idProvider, "User", tenantId, actorId).set("userName", "ted").build());
        ObjectId content1 = write(EventBuilder.create(idProvider, "Content", tenantId, actorId).set("ref_originalAuthor", user1).build());

        expectations.addExpectation(content1, viewClassName, viewFieldName, new ObjectId[]{content1, user1}, "userName", "ted");
        expectations.assertExpectation(tenantIdAndCentricId);
        expectations.clear();

        ObjectNode view = readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, content1.getId()));
        Assert.assertNotNull(view);

        write(EventBuilder.update(user1, tenantId, actorId).set(ReservedFields.DELETED, true).build());

        view = readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, content1.getId()));
        Assert.assertNull(view);

        write(EventBuilder.update(content1, tenantId, actorId).set("ref_originalAuthor", user1).build());
        view = readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, content1.getId()));
        Assert.assertNull(view);

    }

    @Test
    public void testMidPathDeleted() throws Exception {
        String viewClassName = "Values";
        String viewFieldName = "userInfo";
        Expectations expectations = initModelPaths(viewClassName + "::" + viewFieldName
                + "::Version.ref_parent.ref.Content|Content.ref_originalAuthor.ref.User|User.userName");

        ObjectId user1 = write(EventBuilder.create(idProvider, "User", tenantId, actorId).set("userName", "ted").build());
        ObjectId content1 = write(EventBuilder.create(idProvider, "Content", tenantId, actorId).set("ref_originalAuthor", user1).build());
        ObjectId version1 = write(EventBuilder.create(idProvider, "Version", tenantId, actorId).set("ref_parent", content1).build());

        expectations.addExpectation(version1, viewClassName, viewFieldName, new ObjectId[]{version1, content1, user1}, "userName", "ted");
        expectations.assertExpectation(tenantIdAndCentricId);
        expectations.clear();

        ObjectNode view = readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, version1.getId()));
        Assert.assertNotNull(view);

        // - 3
        write(EventBuilder.update(content1, tenantId, actorId).set(ReservedFields.DELETED, true).build());

        view = readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, version1.getId()));
        Assert.assertNull(view);

        write(EventBuilder.update(user1, tenantId, actorId).set("userName", "ted").build());
        view = readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, version1.getId()));
        Assert.assertNull(view);

        write(EventBuilder.update(version1, tenantId, actorId).set("ref_parent", content1).build());
        view = readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, version1.getId()));
        Assert.assertNull(view);

    }

    @Test
    public void testHeadOfPathDeleted() throws Exception {
        String viewClassName = "Values";
        String viewFieldName = "userInfo";
        Expectations expectations = initModelPaths(viewClassName + "::" + viewFieldName
                + "::Version.ref_parent.ref.Content|Content.ref_originalAuthor.ref.User|User.userName");

        ObjectId user1 = write(EventBuilder.create(idProvider, "User", tenantId, actorId).set("userName", "ted").build());
        ObjectId content1 = write(EventBuilder.create(idProvider, "Content", tenantId, actorId).set("ref_originalAuthor", user1).build());
        ObjectId version1 = write(EventBuilder.create(idProvider, "Version", tenantId, actorId).set("ref_parent", content1).build());

        expectations.addExpectation(version1, viewClassName, viewFieldName, new ObjectId[]{version1, content1, user1}, "userName", "ted");
        expectations.assertExpectation(tenantIdAndCentricId);
        expectations.clear();

        ObjectNode view = readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, version1.getId()));
        Assert.assertNotNull(view);

        write(EventBuilder.update(version1, tenantId, actorId).set(ReservedFields.DELETED, true).build());

        view = readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, version1.getId()));
        Assert.assertNull(view);

        write(EventBuilder.update(user1, tenantId, actorId).set("userName", "ted").build());
        view = readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, content1.getId()));
        Assert.assertNull(view);

        write(EventBuilder.update(content1, tenantId, actorId).set("ref_originalAuthor", user1).build());
        view = readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, content1.getId()));
        Assert.assertNull(view);

    }

    @Test
    public void testTailOfPathDeleted() throws Exception {
        String viewClassName = "Values";
        String viewFieldName = "userInfo";
        Expectations expectations = initModelPaths(viewClassName + "::" + viewFieldName
                + "::Version.ref_parent.ref.Content|Content.ref_originalAuthor.ref.User|User.userName");

        ObjectId user1 = write(EventBuilder.create(idProvider, "User", tenantId, actorId).set("userName", "ted").build());
        ObjectId content1 = write(EventBuilder.create(idProvider, "Content", tenantId, actorId).set("ref_originalAuthor", user1).build());
        ObjectId version1 = write(EventBuilder.create(idProvider, "Version", tenantId, actorId).set("ref_parent", content1).build());

        expectations.addExpectation(version1, viewClassName, viewFieldName, new ObjectId[]{version1, content1, user1}, "userName", "ted");
        expectations.assertExpectation(tenantIdAndCentricId);
        expectations.clear();

        ObjectNode view = readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, version1.getId()));
        Assert.assertNotNull(view);

        write(EventBuilder.update(user1, tenantId, actorId).set(ReservedFields.DELETED, true).build());

        view = readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, version1.getId()));
        Assert.assertNull(view);

        write(EventBuilder.update(content1, tenantId, actorId).set("ref_originalAuthor", user1).build());
        view = readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, content1.getId()));
        Assert.assertNull(view);

        write(EventBuilder.update(version1, tenantId, actorId).set("ref_parent", content1).build());
        view = readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, content1.getId()));
        Assert.assertNull(view);
    }

    @Test
    public void testBackRefMidPathDeleted() throws Exception {
        String viewClassName = "Values";
        String viewFieldName = "userInfo";
        String path = viewClassName + "::" + viewFieldName
                + "::Version.backRefs.Content.ref_version|Content.backRefs.User.ref_content|User.userName";
        Expectations expectations = initModelPaths(path);

        ObjectId version1 = write(EventBuilder.create(idProvider, "Version", tenantId, actorId).build());
        ObjectId content1 = write(EventBuilder
                .create(idProvider, "Content", tenantId, actorId).set("ref_version", version1).build());
        ObjectId user1 = write(EventBuilder
                .create(idProvider, "User", tenantId, actorId).set("userName", "ted").set("ref_content", content1).build());

        expectations.addExpectation(version1, viewClassName, viewFieldName, new ObjectId[]{version1, content1, user1}, "userName", "ted");
        expectations.assertExpectation(tenantIdAndCentricId);
        expectations.clear();

        ObjectNode view = readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, version1.getId()));
        System.out.println("Expect Not Null:" + view);
        Assert.assertNotNull(view);

        write(EventBuilder.update(content1, tenantId, actorId).set(ReservedFields.DELETED, true).build());

        view = readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, version1.getId()));
        System.out.println("Expect Null:" + view);
        Assert.assertNull(view);

        write(EventBuilder.update(user1, tenantId, actorId).set("userName", "ted").set("ref_content", content1).build());
        view = readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, content1.getId()));
        Assert.assertNull(view);

    }

    @Test
    public void testBackRefHeadOfPathDeleted() throws Exception {
        String viewClassName = "Values";
        String viewFieldName = "userInfo";
        String path = viewClassName + "::" + viewFieldName
                + "::Version.backRefs.Content.ref_version|Content.backRefs.User.ref_content|User.userName";
        Expectations expectations = initModelPaths(path);

        ObjectId version1 = write(EventBuilder.create(idProvider, "Version", tenantId, actorId).build());
        ObjectId content1 = write(EventBuilder.create(idProvider, "Content", tenantId, actorId).set("ref_version", version1).build());
        ObjectId user1 = write(EventBuilder
                .create(idProvider, "User", tenantId, actorId).set("userName", "ted").set("ref_content", content1).build());

        ObjectNode view = readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, version1.getId()));
        Assert.assertNotNull(view);

        expectations.addExpectation(version1, viewClassName, viewFieldName, new ObjectId[]{version1, content1, user1}, "userName", "ted");
        expectations.assertExpectation(tenantIdAndCentricId);
        expectations.clear();

        write(EventBuilder.update(version1, tenantId, actorId).set(ReservedFields.DELETED, true).build());

        view = readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, version1.getId()));
        Assert.assertNull(view);

        write(EventBuilder.update(content1, tenantId, actorId).set("ref_version", version1).build());
        view = readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, content1.getId()));
        Assert.assertNull(view);

        write(EventBuilder.update(user1, tenantId, actorId).set("userName", "ted").set("ref_content", content1).build());
        view = readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, content1.getId()));
        Assert.assertNull(view);

    }

    @Test
    public void testBackRefTailOfPathDeleted() throws Exception {
        String viewClassName = "Values";
        String viewFieldName = "userInfo";
        String path = viewClassName + "::" + viewFieldName
                + "::Version.backRefs.Content.ref_version|Content.backRefs.User.ref_content|User.userName";
        Expectations expectations = initModelPaths(path);

        ObjectId version1 = write(EventBuilder.create(idProvider, "Version", tenantId, actorId).build());
        ObjectId content1 = write(EventBuilder.create(idProvider, "Content", tenantId, actorId).set("ref_version", version1).build());
        ObjectId user1 = write(EventBuilder
                .create(idProvider, "User", tenantId, actorId).set("userName", "ted").set("ref_content", content1).build());

        expectations.addExpectation(version1, viewClassName, viewFieldName, new ObjectId[]{version1, content1, user1}, "userName", "ted");
        expectations.assertExpectation(tenantIdAndCentricId);
        expectations.clear();

        ObjectNode view = readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, version1.getId()));
        Assert.assertNotNull(view);

        write(EventBuilder.update(user1, tenantId, actorId).set(ReservedFields.DELETED, true).build());

        view = readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, version1.getId()));
        Assert.assertNull(view, " view = " + view);

        write(EventBuilder.update(content1, tenantId, actorId).set("ref_version", version1).build());
        view = readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, content1.getId()));
        Assert.assertNull(view);
    }

    @Test
    public void testLatestBackRefMidPathDeleted() throws Exception {
        String viewClassName = "Values";
        String viewFieldName = "userInfo";
        String path = viewClassName + "::" + viewFieldName
                + "::Version.latest_backRef.Content.ref_version|Content.latest_backRef.User.ref_content|User.userName";
        Expectations expectations = initModelPaths(path);

        ObjectId version1 = write(EventBuilder.create(idProvider, "Version", tenantId, actorId).build());
        ObjectId content1 = write(EventBuilder
                .create(idProvider, "Content", tenantId, actorId).set("ref_version", version1).build());
        ObjectId user1 = write(EventBuilder
                .create(idProvider, "User", tenantId, actorId).set("userName", "ted").set("ref_content", content1).build());

        expectations.addExpectation(version1, viewClassName, viewFieldName, new ObjectId[]{version1, content1, user1}, "userName", "ted");
        expectations.assertExpectation(tenantIdAndCentricId);
        expectations.clear();

        ObjectNode view = readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, version1.getId()));
        Assert.assertNotNull(view);

        write(EventBuilder.update(content1, tenantId, actorId).set(ReservedFields.DELETED, true).build());

        view = readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, version1.getId()));
        Assert.assertNull(view);

        write(EventBuilder.update(version1, tenantId, actorId).set("ref_parent", content1).build());
        view = readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, content1.getId()));
        Assert.assertNull(view);

        write(EventBuilder.update(user1, tenantId, actorId).set("userName", "ted").set("ref_content", content1).build());
        view = readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, content1.getId()));
        Assert.assertNull(view);

    }

    @Test
    public void testLatestBackRefHeadOfPathDeleted() throws Exception {
        String viewClassName = "Values";
        String viewFieldName = "userInfo";
        String path = viewClassName + "::" + viewFieldName
                + "::Version.latest_backRef.Content.ref_version|Content.latest_backRef.User.ref_content|User.userName";
        Expectations expectations = initModelPaths(path);

        ObjectId version1 = write(EventBuilder.create(idProvider, "Version", tenantId, actorId).build());
        ObjectId content1 = write(EventBuilder.create(idProvider, "Content", tenantId, actorId).set("ref_version", version1).build());
        ObjectId user1 = write(EventBuilder
                .create(idProvider, "User", tenantId, actorId).set("userName", "ted").set("ref_content", content1).build());

        expectations.addExpectation(version1, viewClassName, viewFieldName, new ObjectId[]{version1, content1, user1}, "userName", "ted");
        expectations.assertExpectation(tenantIdAndCentricId);
        expectations.clear();

        ObjectNode view = readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, version1.getId()));
        Assert.assertNotNull(view);

        write(EventBuilder.update(version1, tenantId, actorId).set(ReservedFields.DELETED, true).build());

        view = readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, version1.getId()));
        Assert.assertNull(view);

        write(EventBuilder.update(content1, tenantId, actorId).set("ref_version", version1).build());
        view = readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, content1.getId()));
        Assert.assertNull(view);

        write(EventBuilder.update(user1, tenantId, actorId).set("userName", "ted").set("ref_content", content1).build());
        view = readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, content1.getId()));
        Assert.assertNull(view);

    }

    @Test
    public void testLatestBackRefTailOfPathDeleted() throws Exception {
        String viewClassName = "Values";
        String viewFieldName = "userInfo";
        String path = viewClassName + "::" + viewFieldName
                + "::Version.latest_backRef.Content.ref_version|Content.latest_backRef.User.ref_content|User.userName";
        Expectations expectations = initModelPaths(path);

        ObjectId version1 = write(EventBuilder.create(idProvider, "Version", tenantId, actorId).build());
        ObjectId content1 = write(EventBuilder.create(idProvider, "Content", tenantId, actorId).set("ref_version", version1).build());
        ObjectId user1 = write(EventBuilder
                .create(idProvider, "User", tenantId, actorId).set("userName", "ted").set("ref_content", content1).build());

        expectations.addExpectation(version1, viewClassName, viewFieldName, new ObjectId[]{version1, content1, user1}, "userName", "ted");
        expectations.assertExpectation(tenantIdAndCentricId);
        expectations.clear();

        ObjectNode view = readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, version1.getId()));
        Assert.assertNotNull(view);

        write(EventBuilder.update(user1, tenantId, actorId).set(ReservedFields.DELETED, true).build());

        view = readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, version1.getId()));
        Assert.assertNull(view, " view = " + view);

        write(EventBuilder.update(content1, tenantId, actorId).set("ref_version", version1).build());
        view = readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, content1.getId()));
        Assert.assertNull(view);

        write(EventBuilder.update(version1, tenantId, actorId).set("ref_parent", content1).build());
        view = readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, content1.getId()));
        Assert.assertNull(view);

    }

    @Test
    public void testRefsMidPathDeleted() throws Exception {
        String viewClassName = "Values";
        String viewFieldName = "userInfo";
        Expectations expectations = initModelPaths(viewClassName + "::" + viewFieldName
                + "::Version.refs_parent.refs.Content|Content.refs_originalAuthor.refs.User|User.userName");

        ObjectId user1 = write(EventBuilder.create(idProvider, "User", tenantId, actorId).set("userName", "ted").build());
        ObjectId content1 = write(EventBuilder.create(idProvider, "Content", tenantId, actorId).set("refs_originalAuthor", Arrays.asList(user1))
                .build());
        ObjectId version1 = write(EventBuilder.create(idProvider, "Version", tenantId, actorId)
                .set("refs_parent", Arrays.asList(content1)).build());

        expectations.addExpectation(version1, viewClassName, viewFieldName, new ObjectId[]{version1, content1, user1}, "userName", "ted");
        expectations.assertExpectation(tenantIdAndCentricId);
        expectations.clear();

        ObjectNode view = readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, version1.getId()));
        Assert.assertNotNull(view);

        write(EventBuilder.update(content1, tenantId, actorId).set(ReservedFields.DELETED, true).build());

        view = readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, version1.getId()));
        Assert.assertNull(view);

        write(EventBuilder.update(version1, tenantId, actorId).set("ref_parent", Arrays.asList(content1)).build());
        view = readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, content1.getId()));
        Assert.assertNull(view);

        write(EventBuilder.update(user1, tenantId, actorId).set("userName", "ted").build());
        view = readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, content1.getId()));
        Assert.assertNull(view);

    }

    @Test
    public void testRefsHeadOfPathDeleted() throws Exception {
        String viewClassName = "Values";
        String viewFieldName = "userInfo";
        Expectations expectations = initModelPaths(viewClassName + "::" + viewFieldName
                + "::Version.refs_parent.refs.Content|Content.refs_originalAuthor.refs.User|User.userName");
        ObjectId user1 = write(EventBuilder.create(idProvider, "User", tenantId, actorId).set("userName", "ted").build());
        ObjectId content1 = write(EventBuilder.create(idProvider, "Content", tenantId, actorId).set("refs_originalAuthor", Arrays.asList(user1))
                .build());
        ObjectId version1 = write(EventBuilder.create(idProvider, "Version", tenantId, actorId)
                .set("refs_parent", Arrays.asList(content1)).build());

        expectations.addExpectation(version1, viewClassName, viewFieldName, new ObjectId[]{version1, content1, user1}, "userName", "ted");
        expectations.assertExpectation(tenantIdAndCentricId);
        expectations.clear();

        ObjectNode view = readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, version1.getId()));
        Assert.assertNotNull(view);

        write(EventBuilder.update(version1, tenantId, actorId).set(ReservedFields.DELETED, true).build());

        view = readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, version1.getId()));
        Assert.assertNull(view);

        write(EventBuilder.update(user1, tenantId, actorId).set("userName", "ted").build());
        view = readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, content1.getId()));
        Assert.assertNull(view);

        write(EventBuilder.update(content1, tenantId, actorId).set("refs_originalAuthor", Arrays.asList(user1)).build());
        view = readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, content1.getId()));
        Assert.assertNull(view);

    }

    @Test
    public void testRefsTailOfPathDeleted() throws Exception {
        String viewClassName = "Values";
        String viewFieldName = "userInfo";
        Expectations expectations = initModelPaths(viewClassName + "::" + viewFieldName
                + "::Version.refs_parent.refs.Content|Content.refs_originalAuthor.refs.User|User.userName");
        ObjectId user1 = write(EventBuilder.create(idProvider, "User", tenantId, actorId).set("userName", "ted").build());
        ObjectId content1 = write(EventBuilder.create(idProvider, "Content", tenantId, actorId).set("refs_originalAuthor", Arrays.asList(user1))
                .build());
        ObjectId version1 = write(EventBuilder.create(idProvider, "Version", tenantId, actorId)
                .set("refs_parent", Arrays.asList(content1)).build());

        expectations.addExpectation(version1, viewClassName, viewFieldName, new ObjectId[]{version1, content1, user1}, "userName", "ted");
        expectations.assertExpectation(tenantIdAndCentricId);
        expectations.clear();

        ObjectNode view = readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, version1.getId()));
        Assert.assertNotNull(view);

        write(EventBuilder.update(user1, tenantId, actorId).set(ReservedFields.DELETED, true).build());

        view = readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, version1.getId()));
        Assert.assertNull(view);

        write(EventBuilder.update(content1, tenantId, actorId).set("refs_originalAuthor", Arrays.asList(user1)).build());
        view = readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, content1.getId()));
        Assert.assertNull(view);

        write(EventBuilder.update(version1, tenantId, actorId).set("refs_parent", Arrays.asList(content1)).build());
        view = readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, content1.getId()));
        Assert.assertNull(view);
    }

    @Test()
    public void testDeleteRootWithInitialBackRefOnly() throws Exception {
        String viewClass = "ViewToDelete";
        String viewClass2 = "AnotherViewToDelete";
        Expectations expectations = initModelPaths(
                viewClass + "::path4::Document.latest_backRef.Tag.ref_tagged|Tag.name",
                viewClass2 + "::path5::Document.latest_backRef.Tag.ref_tagged|Tag.name");

        ObjectId docId = write(EventBuilder.create(idProvider, "Document", tenantId, actorId).build());
        ObjectId tagId = write(EventBuilder.create(idProvider, "Tag", tenantId, actorId)
                .set("ref_tagged", docId)
                .set("name", "foo")
                .build());

        expectations.addExpectation(docId, viewClass, "path4", new ObjectId[]{docId, tagId}, "name", "foo");
        expectations.addExpectation(docId, viewClass2, "path5", new ObjectId[]{docId, tagId}, "name", "foo");

        expectations.assertExpectation(tenantIdAndCentricId);
        expectations.clear();

        ObjectNode view = readView(tenantIdAndCentricId, actorId, new ObjectId(viewClass, docId.getId()));
        Assert.assertNotNull(view);

        write(EventBuilder.update(docId, tenantId, actorId).set(ReservedFields.DELETED, true).build());

        expectations.addExpectation(docId, viewClass, "path4", new ObjectId[]{docId, tagId}, "name", null);
        expectations.addExpectation(docId, viewClass2, "path5", new ObjectId[]{docId, tagId}, "name", null);

        expectations.assertExpectation(tenantIdAndCentricId);
        expectations.clear();

        view = readView(tenantIdAndCentricId, actorId, new ObjectId(viewClass, docId.getId()));
        Assert.assertNull(view);

        write(EventBuilder.update(tagId, tenantId, actorId).set("ref_tagged", docId).build());
        view = readView(tenantIdAndCentricId, actorId, new ObjectId(viewClass, docId.getId()));
        Assert.assertNull(view);

    }

    @Test
    public void testThreeLevelsHeadDeleted() throws Exception {
        String viewClass = "ViewToDelete";
        Expectations expectations = initModelPaths(viewClass + "::pathID::Document.latest_backRef.Tag.ref_tagged|Tag.tagger.ref.User|User.firstName",
                viewClass + "::pathID2::Document.latest_backRef.Tag.ref_tagged|Tag.tagValue",
                viewClass + "::pathID3::Document.title");

        ObjectId docId = write(EventBuilder.create(idProvider, "Document", tenantId, actorId).set("title", "booya").build());
        ObjectId userId = write(EventBuilder.create(idProvider, "User", tenantId, actorId).set("firstName", "Larry").build());
        ObjectId tagId = write(EventBuilder.create(idProvider, "Tag", tenantId, actorId).set("ref_tagged", docId).set("tagValue", "blah").
                set("tagger", userId).build());

        expectations.addExpectation(docId, viewClass, "pathID", new ObjectId[]{docId, tagId, userId}, "firstName", "Larry");
        expectations.addExpectation(docId, viewClass, "pathID2", new ObjectId[]{docId, tagId}, "tagValue", "blah");
        expectations.addExpectation(docId, viewClass, "pathID3", new ObjectId[]{docId}, "title", "booya");

        expectations.assertExpectation(tenantIdAndCentricId);
        expectations.clear();

        ObjectNode view = readView(tenantIdAndCentricId, actorId, new ObjectId(viewClass, docId.getId()));

        Assert.assertNotNull(view);

        System.out.println("View to delete:\n" + mapper.writeValueAsString(view));

        write(EventBuilder.update(docId, tenantId, actorId).set(ReservedFields.DELETED, true).build());

        expectations.addExpectation(docId, viewClass, "pathID", new ObjectId[]{docId, tagId, userId}, "firstName", null);
        expectations.addExpectation(docId, viewClass, "pathID2", new ObjectId[]{docId, tagId}, "tagValue", null);
        expectations.addExpectation(docId, viewClass, "pathID3", new ObjectId[]{docId}, "title", null);

        expectations.assertExpectation(tenantIdAndCentricId);
        expectations.clear();

        view = readView(tenantIdAndCentricId, actorId, new ObjectId(viewClass, docId.getId()));
        Assert.assertNull(view);

        write(EventBuilder.update(userId, tenantId, actorId).set("firstName", "Larry").build());
        view = readView(tenantIdAndCentricId, actorId, new ObjectId(viewClass, docId.getId()));
        Assert.assertNull(view);

        write(EventBuilder.update(tagId, tenantId, actorId).set("ref_tagged", docId).set("tagValue", "blah").set("tagger", userId).build());
        view = readView(tenantIdAndCentricId, actorId, new ObjectId(viewClass, docId.getId()));
        Assert.assertNull(view);

    }

    @Test
    public void testThreeLevelsMidDeleted() throws Exception {
        String viewClass = "ViewToDelete";
        Expectations expectations = initModelPaths(viewClass + "::pathID::Document.latest_backRef.Tag.ref_tagged|Tag.tagger.ref.User|User.firstName",
                viewClass + "::pathID2::Document.latest_backRef.Tag.ref_tagged|Tag.tagValue",
                viewClass + "::pathID3::Document.title");

        ObjectId docId = write(EventBuilder.create(idProvider, "Document", tenantId, actorId).set("title", "booya").build());
        ObjectId userId = write(EventBuilder.create(idProvider, "User", tenantId, actorId).set("firstName", "Larry").build());
        ObjectId tagId = write(EventBuilder.create(idProvider, "Tag", tenantId, actorId).set("ref_tagged", docId).set("tagValue", "blah").
                set("tagger", userId).build());

        expectations.addExpectation(docId, viewClass, "pathID", new ObjectId[]{docId, tagId, userId}, "firstName", "Larry");
        expectations.addExpectation(docId, viewClass, "pathID2", new ObjectId[]{docId, tagId}, "tagValue", "blah");
        expectations.addExpectation(docId, viewClass, "pathID3", new ObjectId[]{docId}, "title", "booya");

        expectations.assertExpectation(tenantIdAndCentricId);
        expectations.clear();

        ObjectNode view = readView(tenantIdAndCentricId, actorId, new ObjectId(viewClass, docId.getId()));

        Assert.assertNotNull(view);

        write(EventBuilder.update(tagId, tenantId, actorId).set(ReservedFields.DELETED, true).build());

        expectations.addExpectation(docId, viewClass, "pathID", new ObjectId[]{docId, tagId, userId}, "firstName", null);
        expectations.addExpectation(docId, viewClass, "pathID2", new ObjectId[]{docId, tagId}, "tagValue", null);
        expectations.addExpectation(docId, viewClass, "pathID3", new ObjectId[]{docId}, "title", "booya");

        expectations.assertExpectation(tenantIdAndCentricId);
        expectations.clear();

        view = readView(tenantIdAndCentricId, actorId, new ObjectId(viewClass, docId.getId()));
        Assert.assertNotNull(view);

        Assert.assertFalse(view.has("latest_ref_tagged"));

        System.out.println("View with mid path deleted:\n" + mapper.writeValueAsString(view));
    }

    @Test
    public void testThreeLevelsLeafDeleted() throws Exception {
        String viewClass = "ViewToDelete";
        Expectations expectations = initModelPaths(viewClass + "::pathID::Document.latest_backRef.Tag.ref_tagged|Tag.tagger.ref.User|User.firstName",
                viewClass + "::pathID2::Document.latest_backRef.Tag.ref_tagged|Tag.tagValue",
                viewClass + "::pathID3::Document.title");

        ObjectId docId = write(EventBuilder.create(idProvider, "Document", tenantId, actorId).set("title", "booya").build());
        ObjectId userId = write(EventBuilder.create(idProvider, "User", tenantId, actorId).set("firstName", "Larry").build());
        ObjectId tagId = write(EventBuilder.create(idProvider, "Tag", tenantId, actorId).set("ref_tagged", docId).set("tagValue", "blah").
                set("tagger", userId).build());

        expectations.addExpectation(docId, viewClass, "pathID", new ObjectId[]{docId, tagId, userId}, "firstName", "Larry");
        expectations.addExpectation(docId, viewClass, "pathID2", new ObjectId[]{docId, tagId}, "tagValue", "blah");
        expectations.addExpectation(docId, viewClass, "pathID3", new ObjectId[]{docId}, "title", "booya");

        expectations.assertExpectation(tenantIdAndCentricId);
        expectations.clear();

        ObjectNode view = readView(tenantIdAndCentricId, actorId, new ObjectId(viewClass, docId.getId()));

        Assert.assertNotNull(view);

        System.out.println("View to delete:\n" + mapper.writeValueAsString(view));

        write(EventBuilder.update(userId, tenantId, actorId).set(ReservedFields.DELETED, true).build());

        expectations.addExpectation(docId, viewClass, "pathID", new ObjectId[]{docId, tagId, userId}, "firstName", null);
        expectations.addExpectation(docId, viewClass, "pathID2", new ObjectId[]{docId, tagId}, "tagValue", "blah");
        expectations.addExpectation(docId, viewClass, "pathID3", new ObjectId[]{docId}, "title", "booya");

        expectations.assertExpectation(tenantIdAndCentricId);
        expectations.clear();

        view = readView(tenantIdAndCentricId, actorId, new ObjectId(viewClass, docId.getId()));
        Assert.assertNotNull(view);

        Assert.assertTrue(view.has("latest_ref_tagged"));

        JsonNode tagNode = view.get("latest_ref_tagged");

        Assert.assertNotNull(tagNode);

        Assert.assertFalse(tagNode.has("tagger"));

        System.out.println("View with leaf deleted:\n" + mapper.writeValueAsString(view));
    }

    @Test
    public void testDeleteRefWithMultipleSubRefs() throws Exception {
        String viewClassName = "ActivityView";
        initModelPaths(
                viewClassName + "::path0::Activity.verbSubjectEventId",
                viewClassName + "::path1::Activity.verbSubject.ref.CommentVersion|CommentVersion.author.ref.User|User.firstName",
                viewClassName + "::path2::Activity.verbSubject.ref.CommentVersion|CommentVersion.activityParent.ref.Document|Document.subject");

        ObjectId author = write(EventBuilder.create(idProvider, "User", tenantId, actorId)
                .set("firstName", "John")
                .set("lastName", "Doe")
                .build());

        ObjectId document = write(EventBuilder.create(idProvider, "Document", tenantId, actorId)
                .set("subject", "Subject")
                .build());

        ObjectId verbSubject = write(EventBuilder.create(idProvider, "CommentVersion", tenantId, actorId)
                .set("author", author.toStringForm())
                .set("activityParent", document.toStringForm())
                .build());

        ObjectId activity = write(EventBuilder.create(idProvider, "Activity", tenantId, actorId)
                .set("verbSubject", verbSubject.toStringForm())
                .set("verbSubjectEventId", "12345")
                .build());

        ObjectNode view = readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, activity.getId()));
        System.out.println(mapper.writeValueAsString(view));
        System.out.flush();

        Assert.assertNotNull(view);
        Assert.assertNotNull(view.get("verbSubject"));

        // Delete the verb subject
        write(EventBuilder.update(verbSubject, tenantId, actorId).set(ReservedFields.DELETED, true).build());

        view = readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, activity.getId()));
        System.out.println(mapper.writeValueAsString(view));
        System.out.flush();

        Assert.assertNotNull(view);
        Assert.assertNull(view.get("verbSubject"));
    }

    @Test
    public void testThreeLevelsDeleteTailAndReLink() throws Exception {
        //LogManager.getRootLogger().setLevel(Level.TRACE);
        String viewClassName = "3Levels";
        String pathId = "path";
        Expectations expectations = initModelPaths(
                viewClassName + "::" + pathId + "::CommentVersion.parent.ref.Comment|Comment.author.ref.User|User.firstName");

        ObjectId author = write(EventBuilder.create(idProvider, "User", tenantId, actorId)
                .set("firstName", "John")
                .build());

        ObjectId comment = write(EventBuilder.create(idProvider, "Comment", tenantId, actorId)
                .set("author", author)
                .build());

        ObjectId commentVersion = write(EventBuilder.create(idProvider, "CommentVersion", tenantId, actorId)
                .set("parent", comment)
                .build());

        // commentVersion -(parent)-> comment -(author)-> author.firstName
        expectations.addExpectation(commentVersion, viewClassName, pathId, new ObjectId[]{commentVersion, comment, author}, "firstName", "John");
        expectations.assertExpectation(tenantIdAndCentricId);
        expectations.clear();
        ObjectNode view = readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, commentVersion.getId()));
        System.out.println("view:" + view);
        Assert.assertNotNull(view);
        System.out.println("--------------------------------------------------------------------");

        write(EventBuilder.update(author, tenantId, actorId).set(ReservedFields.DELETED, true).build());

        expectations.addExpectation(commentVersion, viewClassName, pathId, new ObjectId[]{commentVersion, comment, author}, "firstName", null);
        expectations.assertExpectation(tenantIdAndCentricId);
        expectations.clear();
        view = readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, commentVersion.getId()));

        System.out.println("view:" + view);
        System.out.println("--------------------------------------------------------------------");
        Assert.assertNull(view);

        write(EventBuilder.update(comment, tenantId, actorId)
                .set("author", author)
                .build());

        expectations.addExpectation(commentVersion, viewClassName, pathId, new ObjectId[]{commentVersion, comment, author}, "firstName", null);
        expectations.assertExpectation(tenantIdAndCentricId);
        expectations.clear();
        view = readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, commentVersion.getId()));
        System.out.println("view:" + view);
        System.out.println("--------------------------------------------------------------------");
        Assert.assertNull(view);

    }

    @Test
    public void testThreeLevelsDeleteMiddleAndReLink() throws Exception {
        String viewClassName = "3Levels";
        String pathId = "path";
        Expectations expectations = initModelPaths(
                viewClassName + "::" + pathId + "::CommentVersion.parent.ref.Comment|Comment.author.ref.User|User.firstName");

        ObjectId author = write(EventBuilder.create(idProvider, "User", tenantId, actorId)
                .set("firstName", "John")
                .build());

        ObjectId comment = write(EventBuilder.create(idProvider, "Comment", tenantId, actorId)
                .set("author", author)
                .build());

        ObjectId commentVersion = write(EventBuilder.create(idProvider, "CommentVersion", tenantId, actorId)
                .set("parent", comment)
                .build());

        expectations.addExpectation(commentVersion, viewClassName, pathId, new ObjectId[]{commentVersion, comment, author}, "firstName", "John");
        expectations.assertExpectation(tenantIdAndCentricId);
        expectations.clear();
        ObjectNode view = readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, commentVersion.getId()));

        Assert.assertNotNull(view);

        write(EventBuilder.update(comment, tenantId, actorId).set(ReservedFields.DELETED, true).build());

        expectations.addExpectation(commentVersion, viewClassName, pathId, new ObjectId[]{commentVersion, comment, author}, "firstName", null);
        expectations.assertExpectation(tenantIdAndCentricId);
        expectations.clear();
        view = readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, commentVersion.getId()));

        Assert.assertNull(view);

        write(EventBuilder.update(commentVersion, tenantId, actorId)
                .set("parent", comment)
                .build());

        //ref between comment and author is gone
        expectations.addExpectation(commentVersion, viewClassName, pathId, new ObjectId[]{commentVersion, comment, author}, "firstName", null);
        expectations.assertExpectation(tenantIdAndCentricId);
        expectations.clear();
        view = readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, commentVersion.getId()));

        Assert.assertNull(view);

    }

    @Test
    public void testThreeLevelsDeleteHeadAndReLink() throws Exception {
        String viewClassName = "3Levels";
        String pathId = "path";
        Expectations expectations = initModelPaths(
                viewClassName + "::" + pathId + "::CommentVersion.parent.ref.Comment|Comment.author.ref.User|User.firstName");

        ObjectId author = write(EventBuilder.create(idProvider, "User", tenantId, actorId)
                .set("firstName", "John")
                .build());

        ObjectId comment = write(EventBuilder.create(idProvider, "Comment", tenantId, actorId)
                .set("author", author)
                .build());

        ObjectId commentVersion = write(EventBuilder.create(idProvider, "CommentVersion", tenantId, actorId)
                .set("parent", comment)
                .build());

        expectations.addExpectation(commentVersion, viewClassName, pathId, new ObjectId[]{commentVersion, comment, author}, "firstName", "John");
        expectations.assertExpectation(tenantIdAndCentricId);
        expectations.clear();
        ObjectNode view = readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, commentVersion.getId()));

        Assert.assertNotNull(view);

        write(EventBuilder.update(commentVersion, tenantId, actorId).set(ReservedFields.DELETED, true).build());

        expectations.addExpectation(commentVersion, viewClassName, pathId, new ObjectId[]{commentVersion, comment, author}, "firstName", null);
        expectations.assertExpectation(tenantIdAndCentricId);
        expectations.clear();
        view = readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, commentVersion.getId()));

        Assert.assertNull(view);

        write(EventBuilder.update(commentVersion, tenantId, actorId)
                .build());

        //ref between comment version and comment is still gone
        expectations.addExpectation(commentVersion, viewClassName, pathId, new ObjectId[]{commentVersion, comment, author}, "firstName", null);
        expectations.assertExpectation(tenantIdAndCentricId);
        expectations.clear();

        view = readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, commentVersion.getId()));
        Assert.assertNull(view);

    }
}
