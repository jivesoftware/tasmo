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
import com.jivesoftware.os.tasmo.model.Views;
import java.util.Arrays;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 *
 */
public class DeletesTest extends BaseTest {

    @Test (dataProvider = "tasmoMaterializer", invocationCount = 1, singleThreaded = true)
    public void testDeletes(TasmoMaterializerHarness t) throws Exception {
        String viewClassName = "Values";
        String viewFieldName = "userInfo";
        Views views = TasmoModelFactory.modelToViews(viewClassName + "::" + viewFieldName + "::User.userName,age");
        t.initModel(views);

        ObjectId user1 = t.write(EventBuilder.create(t.idProvider(), "User", tenantId, actorId).set("userName", "ted").build());

        ObjectNode view = t.readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, user1.getId()));
        t.addExpectation(user1, viewClassName, viewFieldName, new ObjectId[]{user1}, "userName", "ted");
        t.assertExpectation(tenantIdAndCentricId);
        t.clearExpectations();

        Assert.assertNotNull(view);

        // - 3
        t.write(EventBuilder.update(user1, tenantId, actorId).set(ReservedFields.DELETED, true).build());

        view = t.readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, user1.getId()));
        Assert.assertNull(view);

    }

    @Test (dataProvider = "tasmoMaterializer", invocationCount = 1, singleThreaded = true)
    public void testDeleteReferenced(TasmoMaterializerHarness t) throws Exception {
        String viewClassName = "Values";
        String viewFieldName = "userInfo";
        Views views = TasmoModelFactory.modelToViews(viewClassName + "::" + viewFieldName + "::Content.ref_originalAuthor.ref.User|User.userName");
        t.initModel(views);

        ObjectId user1 = t.write(EventBuilder.create(t.idProvider(), "User", tenantId, actorId).set("userName", "ted").build());
        ObjectId content1 = t.write(EventBuilder.create(t.idProvider(), "Content", tenantId, actorId).set("ref_originalAuthor", user1).build());

        t.readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, content1.getId()));
        t.addExpectation(content1, viewClassName, viewFieldName, new ObjectId[]{content1, user1}, "userName", "ted");
        t.assertExpectation(tenantIdAndCentricId);
        t.clearExpectations();

        ObjectNode view = t.readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, content1.getId()));
        Assert.assertNotNull(view);

        t.write(EventBuilder.update(user1, tenantId, actorId).set(ReservedFields.DELETED, true).build());

        view = t.readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, content1.getId()));
        Assert.assertNull(view);

        t.write(EventBuilder.update(content1, tenantId, actorId).set("ref_originalAuthor", user1).build());
        view = t.readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, content1.getId()));
        Assert.assertNull(view);

    }

    @Test (dataProvider = "tasmoMaterializer", invocationCount = 1, singleThreaded = true)
    public void testMidPathDeleted(TasmoMaterializerHarness t) throws Exception {
        String viewClassName = "Values";
        String viewFieldName = "userInfo";
        Views views = TasmoModelFactory.modelToViews(viewClassName + "::" + viewFieldName
                + "::Version.ref_parent.ref.Content|Content.ref_originalAuthor.ref.User|User.userName");
        t.initModel(views);

        ObjectId user1 = t.write(EventBuilder.create(t.idProvider(), "User", tenantId, actorId).set("userName", "ted").build());
        ObjectId content1 = t.write(EventBuilder.create(t.idProvider(), "Content", tenantId, actorId).set("ref_originalAuthor", user1).build());
        ObjectId version1 = t.write(EventBuilder.create(t.idProvider(), "Version", tenantId, actorId).set("ref_parent", content1).build());

        ObjectNode view = t.readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, version1.getId()));
        t.addExpectation(version1, viewClassName, viewFieldName, new ObjectId[]{version1, content1, user1}, "userName", "ted");
        t.readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, version1.getId()));
        t.assertExpectation(tenantIdAndCentricId);
        t.clearExpectations();

        Assert.assertNotNull(view);

        // - 3
        t.write(EventBuilder.update(content1, tenantId, actorId).set(ReservedFields.DELETED, true).build());

        view = t.readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, version1.getId()));
        Assert.assertNull(view);

        t.write(EventBuilder.update(user1, tenantId, actorId).set("userName", "ted").build());
        view = t.readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, version1.getId()));
        Assert.assertNull(view);

        t.write(EventBuilder.update(version1, tenantId, actorId).set("ref_parent", content1).build());
        view = t.readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, version1.getId()));
        Assert.assertNull(view);

    }

    @Test (dataProvider = "tasmoMaterializer", invocationCount = 1, singleThreaded = true)
    public void testHeadOfPathDeleted(TasmoMaterializerHarness t) throws Exception {
        String viewClassName = "Values";
        String viewFieldName = "userInfo";
        Views views = TasmoModelFactory.modelToViews(viewClassName + "::" + viewFieldName
                + "::Version.ref_parent.ref.Content|Content.ref_originalAuthor.ref.User|User.userName");
        t.initModel(views);

        ObjectId user1 = t.write(EventBuilder.create(t.idProvider(), "User", tenantId, actorId).set("userName", "ted").build());
        ObjectId content1 = t.write(EventBuilder.create(t.idProvider(), "Content", tenantId, actorId).set("ref_originalAuthor", user1).build());
        ObjectId version1 = t.write(EventBuilder.create(t.idProvider(), "Version", tenantId, actorId).set("ref_parent", content1).build());

        ObjectNode view = t.readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, version1.getId()));
        t.addExpectation(version1, viewClassName, viewFieldName, new ObjectId[]{version1, content1, user1}, "userName", "ted");
        t.readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, version1.getId()));
        t.assertExpectation(tenantIdAndCentricId);
        t.clearExpectations();

        Assert.assertNotNull(view);

        t.write(EventBuilder.update(version1, tenantId, actorId).set(ReservedFields.DELETED, true).build());

        view = t.readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, version1.getId()));
        Assert.assertNull(view);

        t.write(EventBuilder.update(user1, tenantId, actorId).set("userName", "ted").build());
        view = t.readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, content1.getId()));
        Assert.assertNull(view);

        t.write(EventBuilder.update(content1, tenantId, actorId).set("ref_originalAuthor", user1).build());
        view = t.readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, content1.getId()));
        Assert.assertNull(view);

    }

    @Test (dataProvider = "tasmoMaterializer", invocationCount = 1, singleThreaded = true)
    public void testTailOfPathDeleted(TasmoMaterializerHarness t) throws Exception {
        String viewClassName = "Values";
        String viewFieldName = "userInfo";
        Views views = TasmoModelFactory.modelToViews(viewClassName + "::" + viewFieldName
                + "::Version.ref_parent.ref.Content|Content.ref_originalAuthor.ref.User|User.userName");
        t.initModel(views);

        ObjectId user1 = t.write(EventBuilder.create(t.idProvider(), "User", tenantId, actorId).set("userName", "ted").build());
        ObjectId content1 = t.write(EventBuilder.create(t.idProvider(), "Content", tenantId, actorId).set("ref_originalAuthor", user1).build());
        ObjectId version1 = t.write(EventBuilder.create(t.idProvider(), "Version", tenantId, actorId).set("ref_parent", content1).build());

        ObjectNode view = t.readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, version1.getId()));
        t.addExpectation(version1, viewClassName, viewFieldName, new ObjectId[]{version1, content1, user1}, "userName", "ted");
        t.readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, version1.getId()));
        t.assertExpectation(tenantIdAndCentricId);
        t.clearExpectations();

        Assert.assertNotNull(view);

        t.write(EventBuilder.update(user1, tenantId, actorId).set(ReservedFields.DELETED, true).build());

        view = t.readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, version1.getId()));
        Assert.assertNull(view);

        t.write(EventBuilder.update(content1, tenantId, actorId).set("ref_originalAuthor", user1).build());
        view = t.readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, content1.getId()));
        Assert.assertNull(view);

        t.write(EventBuilder.update(version1, tenantId, actorId).set("ref_parent", content1).build());
        view = t.readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, content1.getId()));
        Assert.assertNull(view);
    }

    @Test (dataProvider = "tasmoMaterializer", invocationCount = 1, singleThreaded = true)
    public void testBackRefMidPathDeleted(TasmoMaterializerHarness t) throws Exception {
        String viewClassName = "Values";
        String viewFieldName = "userInfo";
        String path = viewClassName + "::" + viewFieldName
                + "::Version.backRefs.Content.ref_version|Content.backRefs.User.ref_content|User.userName";
        Views views = TasmoModelFactory.modelToViews(path);
        t.initModel(views);

        ObjectId version1 = t.write(EventBuilder.create(t.idProvider(), "Version", tenantId, actorId).build());
        ObjectId content1 = t.write(EventBuilder
                .create(t.idProvider(), "Content", tenantId, actorId).set("ref_version", version1).build());
        ObjectId user1 = t.write(EventBuilder
                .create(t.idProvider(), "User", tenantId, actorId).set("userName", "ted").set("ref_content", content1).build());

        t.readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, version1.getId()));
        t.addExpectation(version1, viewClassName, viewFieldName, new ObjectId[]{version1, content1, user1}, "userName", "ted");

        t.assertExpectation(tenantIdAndCentricId);
        t.clearExpectations();

        ObjectNode view = t.readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, version1.getId()));
        System.out.println("Expect Not Null:" + view);
        Assert.assertNotNull(view);

        t.write(EventBuilder.update(content1, tenantId, actorId).set(ReservedFields.DELETED, true).build());

        view = t.readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, version1.getId()));
        System.out.println("Expect Null:" + view);
        Assert.assertNull(view);

        t.write(EventBuilder.update(user1, tenantId, actorId).set("userName", "ted").set("ref_content", content1).build());
        view = t.readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, content1.getId()));
        Assert.assertNull(view);

    }

    @Test (dataProvider = "tasmoMaterializer", invocationCount = 1, singleThreaded = true)
    public void testBackRefHeadOfPathDeleted(TasmoMaterializerHarness t) throws Exception {
        String viewClassName = "Values";
        String viewFieldName = "userInfo";
        String path = viewClassName + "::" + viewFieldName
                + "::Version.backRefs.Content.ref_version|Content.backRefs.User.ref_content|User.userName";
        Views views = TasmoModelFactory.modelToViews(path);
        t.initModel(views);

        ObjectId version1 = t.write(EventBuilder.create(t.idProvider(), "Version", tenantId, actorId).build());
        ObjectId content1 = t.write(EventBuilder.create(t.idProvider(), "Content", tenantId, actorId).set("ref_version", version1).build());
        ObjectId user1 = t.write(EventBuilder
                .create(t.idProvider(), "User", tenantId, actorId).set("userName", "ted").set("ref_content", content1).build());

        ObjectNode view = t.readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, version1.getId()));
        Assert.assertNotNull(view);

        t.addExpectation(version1, viewClassName, viewFieldName, new ObjectId[]{version1, content1, user1}, "userName", "ted");
        t.assertExpectation(tenantIdAndCentricId);
        t.clearExpectations();

        t.write(EventBuilder.update(version1, tenantId, actorId).set(ReservedFields.DELETED, true).build());

        view = t.readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, version1.getId()));
        Assert.assertNull(view);

        t.write(EventBuilder.update(content1, tenantId, actorId).set("ref_version", version1).build());
        view = t.readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, content1.getId()));
        Assert.assertNull(view);

        t.write(EventBuilder.update(user1, tenantId, actorId).set("userName", "ted").set("ref_content", content1).build());
        view = t.readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, content1.getId()));
        Assert.assertNull(view);

    }

    @Test (dataProvider = "tasmoMaterializer", invocationCount = 1, singleThreaded = true)
    public void testBackRefTailOfPathDeleted(TasmoMaterializerHarness t) throws Exception {
        String viewClassName = "Values";
        String viewFieldName = "userInfo";
        String path = viewClassName + "::" + viewFieldName
                + "::Version.backRefs.Content.ref_version|Content.backRefs.User.ref_content|User.userName";
        Views views = TasmoModelFactory.modelToViews(path);
        t.initModel(views);
        ObjectId version1 = t.write(EventBuilder.create(t.idProvider(), "Version", tenantId, actorId).build());
        ObjectId content1 = t.write(EventBuilder.create(t.idProvider(), "Content", tenantId, actorId).set("ref_version", version1).build());
        ObjectId user1 = t.write(EventBuilder
                .create(t.idProvider(), "User", tenantId, actorId).set("userName", "ted").set("ref_content", content1).build());

        t.readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, version1.getId()));
        t.addExpectation(version1, viewClassName, viewFieldName, new ObjectId[]{version1, content1, user1}, "userName", "ted");
        t.assertExpectation(tenantIdAndCentricId);
        t.clearExpectations();

        ObjectNode view = t.readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, version1.getId()));
        Assert.assertNotNull(view);

        t.write(EventBuilder.update(user1, tenantId, actorId).set(ReservedFields.DELETED, true).build());

        view = t.readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, version1.getId()));
        Assert.assertNull(view, " view = " + view);

        t.write(EventBuilder.update(content1, tenantId, actorId).set("ref_version", version1).build());
        view = t.readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, content1.getId()));
        Assert.assertNull(view);
    }

    @Test (dataProvider = "tasmoMaterializer", invocationCount = 1, singleThreaded = true)
    public void testLatestBackRefMidPathDeleted(TasmoMaterializerHarness t) throws Exception {
        String viewClassName = "Values";
        String viewFieldName = "userInfo";
        String path = viewClassName + "::" + viewFieldName
                + "::Version.latest_backRef.Content.ref_version|Content.latest_backRef.User.ref_content|User.userName";
        Views views = TasmoModelFactory.modelToViews(path);
        t.initModel(views);

        ObjectId version1 = t.write(EventBuilder.create(t.idProvider(), "Version", tenantId, actorId).build());
        ObjectId content1 = t.write(EventBuilder
                .create(t.idProvider(), "Content", tenantId, actorId).set("ref_version", version1).build());
        ObjectId user1 = t.write(EventBuilder
                .create(t.idProvider(), "User", tenantId, actorId).set("userName", "ted").set("ref_content", content1).build());

        ObjectNode view = t.readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, version1.getId()));
        t.addExpectation(version1, viewClassName, viewFieldName, new ObjectId[]{version1, content1, user1}, "userName", "ted");
        t.readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, version1.getId()));
        t.assertExpectation(tenantIdAndCentricId);
        t.clearExpectations();

        Assert.assertNotNull(view);

        t.write(EventBuilder.update(content1, tenantId, actorId).set(ReservedFields.DELETED, true).build());

        view = t.readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, version1.getId()));
        Assert.assertNull(view);

        t.write(EventBuilder.update(version1, tenantId, actorId).set("ref_parent", content1).build());
        view = t.readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, content1.getId()));
        Assert.assertNull(view);

        t.write(EventBuilder.update(user1, tenantId, actorId).set("userName", "ted").set("ref_content", content1).build());
        view = t.readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, content1.getId()));
        Assert.assertNull(view);

    }

    @Test (dataProvider = "tasmoMaterializer", invocationCount = 1, singleThreaded = true)
    public void testLatestBackRefHeadOfPathDeleted(TasmoMaterializerHarness t) throws Exception {
        String viewClassName = "Values";
        String viewFieldName = "userInfo";
        String path = viewClassName + "::" + viewFieldName
                + "::Version.latest_backRef.Content.ref_version|Content.latest_backRef.User.ref_content|User.userName";
        Views views = TasmoModelFactory.modelToViews(path);
        t.initModel(views);

        ObjectId version1 = t.write(EventBuilder.create(t.idProvider(), "Version", tenantId, actorId).build());
        ObjectId content1 = t.write(EventBuilder.create(t.idProvider(), "Content", tenantId, actorId).set("ref_version", version1).build());
        ObjectId user1 = t.write(EventBuilder
                .create(t.idProvider(), "User", tenantId, actorId).set("userName", "ted").set("ref_content", content1).build());

        ObjectNode view = t.readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, version1.getId()));
        t.addExpectation(version1, viewClassName, viewFieldName, new ObjectId[]{version1, content1, user1}, "userName", "ted");
        t.readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, version1.getId()));
        t.assertExpectation(tenantIdAndCentricId);
        t.clearExpectations();

        Assert.assertNotNull(view);

        t.write(EventBuilder.update(version1, tenantId, actorId).set(ReservedFields.DELETED, true).build());

        view = t.readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, version1.getId()));
        Assert.assertNull(view);

        t.write(EventBuilder.update(content1, tenantId, actorId).set("ref_version", version1).build());
        view = t.readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, content1.getId()));
        Assert.assertNull(view);

        t.write(EventBuilder.update(user1, tenantId, actorId).set("userName", "ted").set("ref_content", content1).build());
        view = t.readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, content1.getId()));
        Assert.assertNull(view);

    }

    @Test (dataProvider = "tasmoMaterializer", invocationCount = 1, singleThreaded = true)
    public void testLatestBackRefTailOfPathDeleted(TasmoMaterializerHarness t) throws Exception {
        String viewClassName = "Values";
        String viewFieldName = "userInfo";
        String path = viewClassName + "::" + viewFieldName
                + "::Version.latest_backRef.Content.ref_version|Content.latest_backRef.User.ref_content|User.userName";
        Views views = TasmoModelFactory.modelToViews(path);
        t.initModel(views);
        ObjectId version1 = t.write(EventBuilder.create(t.idProvider(), "Version", tenantId, actorId).build());
        ObjectId content1 = t.write(EventBuilder.create(t.idProvider(), "Content", tenantId, actorId).set("ref_version", version1).build());
        ObjectId user1 = t.write(EventBuilder
                .create(t.idProvider(), "User", tenantId, actorId).set("userName", "ted").set("ref_content", content1).build());

        ObjectNode view = t.readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, version1.getId()));
        t.addExpectation(version1, viewClassName, viewFieldName, new ObjectId[]{version1, content1, user1}, "userName", "ted");
        t.readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, version1.getId()));
        t.assertExpectation(tenantIdAndCentricId);
        t.clearExpectations();

        Assert.assertNotNull(view);

        t.write(EventBuilder.update(user1, tenantId, actorId).set(ReservedFields.DELETED, true).build());

        view = t.readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, version1.getId()));
        Assert.assertNull(view, " view = " + view);

        t.write(EventBuilder.update(content1, tenantId, actorId).set("ref_version", version1).build());
        view = t.readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, content1.getId()));
        Assert.assertNull(view);

        t.write(EventBuilder.update(version1, tenantId, actorId).set("ref_parent", content1).build());
        view = t.readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, content1.getId()));
        Assert.assertNull(view);

    }

    @Test (dataProvider = "tasmoMaterializer", invocationCount = 1, singleThreaded = true)
    public void testRefsMidPathDeleted(TasmoMaterializerHarness t) throws Exception {
        String viewClassName = "Values";
        String viewFieldName = "userInfo";
        Views views = TasmoModelFactory.modelToViews(viewClassName + "::" + viewFieldName
                + "::Version.refs_parent.refs.Content|Content.refs_originalAuthor.refs.User|User.userName");
        t.initModel(views);

        ObjectId user1 = t.write(EventBuilder.create(t.idProvider(), "User", tenantId, actorId).set("userName", "ted").build());
        ObjectId content1 = t.write(EventBuilder.create(t.idProvider(), "Content", tenantId, actorId).set("refs_originalAuthor", Arrays.asList(user1))
                .build());
        ObjectId version1 = t.write(EventBuilder.create(t.idProvider(), "Version", tenantId, actorId)
                .set("refs_parent", Arrays.asList(content1)).build());

        ObjectNode view = t.readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, version1.getId()));
        t.addExpectation(version1, viewClassName, viewFieldName, new ObjectId[]{version1, content1, user1}, "userName", "ted");
        t.readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, version1.getId()));
        t.assertExpectation(tenantIdAndCentricId);
        t.clearExpectations();

        Assert.assertNotNull(view);

        t.write(EventBuilder.update(content1, tenantId, actorId).set(ReservedFields.DELETED, true).build());

        view = t.readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, version1.getId()));
        Assert.assertNull(view);

        t.write(EventBuilder.update(version1, tenantId, actorId).set("ref_parent", Arrays.asList(content1)).build());
        view = t.readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, content1.getId()));
        Assert.assertNull(view);

        t.write(EventBuilder.update(user1, tenantId, actorId).set("userName", "ted").build());
        view = t.readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, content1.getId()));
        Assert.assertNull(view);

    }

    @Test (dataProvider = "tasmoMaterializer", invocationCount = 1, singleThreaded = true)
    public void testRefsHeadOfPathDeleted(TasmoMaterializerHarness t) throws Exception {
        String viewClassName = "Values";
        String viewFieldName = "userInfo";
        Views views = TasmoModelFactory.modelToViews(viewClassName + "::" + viewFieldName
                + "::Version.refs_parent.refs.Content|Content.refs_originalAuthor.refs.User|User.userName");
        t.initModel(views);

        ObjectId user1 = t.write(EventBuilder.create(t.idProvider(), "User", tenantId, actorId).set("userName", "ted").build());
        ObjectId content1 = t.write(EventBuilder.create(t.idProvider(), "Content", tenantId, actorId).set("refs_originalAuthor", Arrays.asList(user1))
                .build());
        ObjectId version1 = t.write(EventBuilder.create(t.idProvider(), "Version", tenantId, actorId)
                .set("refs_parent", Arrays.asList(content1)).build());

        ObjectNode view = t.readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, version1.getId()));
        t.addExpectation(version1, viewClassName, viewFieldName, new ObjectId[]{version1, content1, user1}, "userName", "ted");
        t.readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, version1.getId()));
        t.assertExpectation(tenantIdAndCentricId);
        t.clearExpectations();

        Assert.assertNotNull(view);

        t.write(EventBuilder.update(version1, tenantId, actorId).set(ReservedFields.DELETED, true).build());

        view = t.readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, version1.getId()));
        Assert.assertNull(view);

        t.write(EventBuilder.update(user1, tenantId, actorId).set("userName", "ted").build());
        view = t.readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, content1.getId()));
        Assert.assertNull(view);

        t.write(EventBuilder.update(content1, tenantId, actorId).set("refs_originalAuthor", Arrays.asList(user1)).build());
        view = t.readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, content1.getId()));
        Assert.assertNull(view);

    }

    @Test (dataProvider = "tasmoMaterializer", invocationCount = 1, singleThreaded = true)
    public void testRefsTailOfPathDeleted(TasmoMaterializerHarness t) throws Exception {
        String viewClassName = "Values";
        String viewFieldName = "userInfo";
        Views views = TasmoModelFactory.modelToViews(viewClassName + "::" + viewFieldName
                + "::Version.refs_parent.refs.Content|Content.refs_originalAuthor.refs.User|User.userName");
        t.initModel(views);

        ObjectId user1 = t.write(EventBuilder.create(t.idProvider(), "User", tenantId, actorId).set("userName", "ted").build());
        ObjectId content1 = t.write(EventBuilder.create(t.idProvider(), "Content", tenantId, actorId).set("refs_originalAuthor", Arrays.asList(user1))
                .build());
        ObjectId version1 = t.write(EventBuilder.create(t.idProvider(), "Version", tenantId, actorId)
                .set("refs_parent", Arrays.asList(content1)).build());

        ObjectNode view = t.readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, version1.getId()));
        t.addExpectation(version1, viewClassName, viewFieldName, new ObjectId[]{version1, content1, user1}, "userName", "ted");
        t.readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, version1.getId()));
        t.assertExpectation(tenantIdAndCentricId);
        t.clearExpectations();

        Assert.assertNotNull(view);

        t.write(EventBuilder.update(user1, tenantId, actorId).set(ReservedFields.DELETED, true).build());

        view = t.readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, version1.getId()));
        Assert.assertNull(view);

        t.write(EventBuilder.update(content1, tenantId, actorId).set("refs_originalAuthor", Arrays.asList(user1)).build());
        view = t.readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, content1.getId()));
        Assert.assertNull(view);

        t.write(EventBuilder.update(version1, tenantId, actorId).set("refs_parent", Arrays.asList(content1)).build());
        view = t.readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, content1.getId()));
        Assert.assertNull(view);
    }

    @Test (dataProvider = "tasmoMaterializer", invocationCount = 1, singleThreaded = true)
    public void testDeleteRootWithInitialBackRefOnly(TasmoMaterializerHarness t) throws Exception {
        String viewClass = "ViewToDelete";
        String viewClass2 = "AnotherViewToDelete";
        Views views = TasmoModelFactory.modelToViews(
                viewClass + "::path4::Document.latest_backRef.Tag.ref_tagged|Tag.name",
                viewClass2 + "::path5::Document.latest_backRef.Tag.ref_tagged|Tag.name");
        t.initModel(views);

        ObjectId docId = t.write(EventBuilder.create(t.idProvider(), "Document", tenantId, actorId).build());
        ObjectId tagId = t.write(EventBuilder.create(t.idProvider(), "Tag", tenantId, actorId)
                .set("ref_tagged", docId)
                .set("name", "foo")
                .build());

        ObjectNode view = t.readView(tenantIdAndCentricId, actorId, new ObjectId(viewClass, docId.getId()));
        t.addExpectation(docId, viewClass, "path4", new ObjectId[]{docId, tagId}, "name", "foo");
        t.addExpectation(docId, viewClass2, "path5", new ObjectId[]{docId, tagId}, "name", "foo");

        t.readView(tenantIdAndCentricId, actorId, new ObjectId(viewClass, docId.getId()));
        t.readView(tenantIdAndCentricId, actorId, new ObjectId(viewClass2, docId.getId()));
        t.assertExpectation(tenantIdAndCentricId);
        t.clearExpectations();

        Assert.assertNotNull(view);

        ObjectNode view2 = t.readView(tenantIdAndCentricId, actorId, new ObjectId(viewClass2, docId.getId()));
        Assert.assertNotNull(view2);

        t.write(EventBuilder.update(docId, tenantId, actorId).set(ReservedFields.DELETED, true).build());

        t.addExpectation(docId, viewClass, "path4", new ObjectId[]{docId, tagId}, "name", null);
        t.addExpectation(docId, viewClass2, "path5", new ObjectId[]{docId, tagId}, "name", null);

        t.readView(tenantIdAndCentricId, actorId, new ObjectId(viewClass, docId.getId()));
        t.readView(tenantIdAndCentricId, actorId, new ObjectId(viewClass2, docId.getId()));
        t.assertExpectation(tenantIdAndCentricId);
        t.clearExpectations();

        view = t.readView(tenantIdAndCentricId, actorId, new ObjectId(viewClass, docId.getId()));
        Assert.assertNull(view);

        view2 = t.readView(tenantIdAndCentricId, actorId, new ObjectId(viewClass2, docId.getId()));
        Assert.assertNull(view2);

        t.write(EventBuilder.update(tagId, tenantId, actorId).set("ref_tagged", docId).build());
        view = t.readView(tenantIdAndCentricId, actorId, new ObjectId(viewClass, docId.getId()));
        System.out.println("view1 = "+mapper.writeValueAsString(view));

        view2 = t.readView(tenantIdAndCentricId, actorId, new ObjectId(viewClass2, docId.getId()));
        System.out.println("view2 = "+mapper.writeValueAsString(view2));

        Assert.assertNull(view);
        Assert.assertNull(view2);

    }

    @Test (dataProvider = "tasmoMaterializer", invocationCount = 1, singleThreaded = true)
    public void testThreeLevelsHeadDeleted(TasmoMaterializerHarness t) throws Exception {
        String viewClass = "ViewToDelete";
        Views views = TasmoModelFactory.modelToViews(viewClass + "::pathID::Document.latest_backRef.Tag.ref_tagged|Tag.tagger.ref.User|User.firstName",
                viewClass + "::pathID2::Document.latest_backRef.Tag.ref_tagged|Tag.tagValue",
                viewClass + "::pathID3::Document.title");
        t.initModel(views);

        ObjectId docId = t.write(EventBuilder.create(t.idProvider(), "Document", tenantId, actorId).set("title", "booya").build());
        ObjectId userId = t.write(EventBuilder.create(t.idProvider(), "User", tenantId, actorId).set("firstName", "Larry").build());
        ObjectId tagId = t.write(EventBuilder.create(t.idProvider(), "Tag", tenantId, actorId).set("ref_tagged", docId).set("tagValue", "blah").
                set("tagger", userId).build());

        ObjectNode view = t.readView(tenantIdAndCentricId, actorId, new ObjectId(viewClass, docId.getId()));
        t.addExpectation(docId, viewClass, "pathID", new ObjectId[]{docId, tagId, userId}, "firstName", "Larry");
        t.addExpectation(docId, viewClass, "pathID2", new ObjectId[]{docId, tagId}, "tagValue", "blah");
        t.addExpectation(docId, viewClass, "pathID3", new ObjectId[]{docId}, "title", "booya");

        t.readView(tenantIdAndCentricId, actorId, new ObjectId(viewClass, docId.getId()));
        t.assertExpectation(tenantIdAndCentricId);
        t.clearExpectations();


        Assert.assertNotNull(view);

        System.out.println("View to delete:\n" + mapper.writeValueAsString(view));

        t.write(EventBuilder.update(docId, tenantId, actorId).set(ReservedFields.DELETED, true).build());

        t.addExpectation(docId, viewClass, "pathID", new ObjectId[]{docId, tagId, userId}, "firstName", null);
        t.addExpectation(docId, viewClass, "pathID2", new ObjectId[]{docId, tagId}, "tagValue", null);
        t.addExpectation(docId, viewClass, "pathID3", new ObjectId[]{docId}, "title", null);

        t.readView(tenantIdAndCentricId, actorId, new ObjectId(viewClass, docId.getId()));
        t.assertExpectation(tenantIdAndCentricId);
        t.clearExpectations();

        view = t.readView(tenantIdAndCentricId, actorId, new ObjectId(viewClass, docId.getId()));
        Assert.assertNull(view);

        t.write(EventBuilder.update(userId, tenantId, actorId).set("firstName", "Larry").build());
        view = t.readView(tenantIdAndCentricId, actorId, new ObjectId(viewClass, docId.getId()));
        Assert.assertNull(view);

        t.write(EventBuilder.update(tagId, tenantId, actorId).set("ref_tagged", docId).set("tagValue", "blah").set("tagger", userId).build());
        view = t.readView(tenantIdAndCentricId, actorId, new ObjectId(viewClass, docId.getId()));
        Assert.assertNull(view);

    }

    @Test (dataProvider = "tasmoMaterializer", invocationCount = 1, singleThreaded = true)
    public void testThreeLevelsMidDeleted(TasmoMaterializerHarness t) throws Exception {
        String viewClass = "ViewToDelete";
        Views views = TasmoModelFactory.modelToViews(viewClass + "::pathID::Document.latest_backRef.Tag.ref_tagged|Tag.tagger.ref.User|User.firstName",
                viewClass + "::pathID2::Document.latest_backRef.Tag.ref_tagged|Tag.tagValue",
                viewClass + "::pathID3::Document.title");
        t.initModel(views);

        ObjectId docId = t.write(EventBuilder.create(t.idProvider(), "Document", tenantId, actorId).set("title", "booya").build());
        ObjectId userId = t.write(EventBuilder.create(t.idProvider(), "User", tenantId, actorId).set("firstName", "Larry").build());
        ObjectId tagId = t.write(EventBuilder.create(t.idProvider(), "Tag", tenantId, actorId).set("ref_tagged", docId).set("tagValue", "blah").
                set("tagger", userId).build());

        ObjectNode view = t.readView(tenantIdAndCentricId, actorId, new ObjectId(viewClass, docId.getId()));
        t.addExpectation(docId, viewClass, "pathID", new ObjectId[]{docId, tagId, userId}, "firstName", "Larry");
        t.addExpectation(docId, viewClass, "pathID2", new ObjectId[]{docId, tagId}, "tagValue", "blah");
        t.addExpectation(docId, viewClass, "pathID3", new ObjectId[]{docId}, "title", "booya");

        t.readView(tenantIdAndCentricId, actorId, new ObjectId(viewClass, docId.getId()));
        t.assertExpectation(tenantIdAndCentricId);
        t.clearExpectations();

        Assert.assertNotNull(view);

        t.write(EventBuilder.update(tagId, tenantId, actorId).set(ReservedFields.DELETED, true).build());

        t.addExpectation(docId, viewClass, "pathID", new ObjectId[]{docId, tagId, userId}, "firstName", null);
        t.addExpectation(docId, viewClass, "pathID2", new ObjectId[]{docId, tagId}, "tagValue", null);
        t.addExpectation(docId, viewClass, "pathID3", new ObjectId[]{docId}, "title", "booya");

        t.readView(tenantIdAndCentricId, actorId, new ObjectId(viewClass, docId.getId()));
        t.assertExpectation(tenantIdAndCentricId);
        t.clearExpectations();

        view = t.readView(tenantIdAndCentricId, actorId, new ObjectId(viewClass, docId.getId()));
        Assert.assertNotNull(view);

        Assert.assertFalse(view.has("latest_ref_tagged"));

        System.out.println("View with mid path deleted:\n" + mapper.writeValueAsString(view));
    }

    @Test (dataProvider = "tasmoMaterializer", invocationCount = 1, singleThreaded = true)
    public void testThreeLevelsLeafDeleted(TasmoMaterializerHarness t) throws Exception {
        String viewClass = "ViewToDelete";
        Views views = TasmoModelFactory.modelToViews(viewClass + "::pathID::Document.latest_backRef.Tag.ref_tagged|Tag.tagger.ref.User|User.firstName",
                viewClass + "::pathID2::Document.latest_backRef.Tag.ref_tagged|Tag.tagValue",
                viewClass + "::pathID3::Document.title");
        t.initModel(views);

        ObjectId docId = t.write(EventBuilder.create(t.idProvider(), "Document", tenantId, actorId).set("title", "booya").build());
        ObjectId userId = t.write(EventBuilder.create(t.idProvider(), "User", tenantId, actorId).set("firstName", "Larry").build());
        ObjectId tagId = t.write(EventBuilder.create(t.idProvider(), "Tag", tenantId, actorId).set("ref_tagged", docId).set("tagValue", "blah").
                set("tagger", userId).build());

        ObjectNode view = t.readView(tenantIdAndCentricId, actorId, new ObjectId(viewClass, docId.getId()));
        t.addExpectation(docId, viewClass, "pathID", new ObjectId[]{docId, tagId, userId}, "firstName", "Larry");
        t.addExpectation(docId, viewClass, "pathID2", new ObjectId[]{docId, tagId}, "tagValue", "blah");
        t.addExpectation(docId, viewClass, "pathID3", new ObjectId[]{docId}, "title", "booya");

        t.readView(tenantIdAndCentricId, actorId, new ObjectId(viewClass, docId.getId()));
        t.assertExpectation(tenantIdAndCentricId);
        t.clearExpectations();

        Assert.assertNotNull(view);

        System.out.println("View to delete:\n" + mapper.writeValueAsString(view));

        t.write(EventBuilder.update(userId, tenantId, actorId).set(ReservedFields.DELETED, true).build());

        t.addExpectation(docId, viewClass, "pathID", new ObjectId[]{docId, tagId, userId}, "firstName", null);
        t.addExpectation(docId, viewClass, "pathID2", new ObjectId[]{docId, tagId}, "tagValue", "blah");
        t.addExpectation(docId, viewClass, "pathID3", new ObjectId[]{docId}, "title", "booya");

        t.readView(tenantIdAndCentricId, actorId, new ObjectId(viewClass, docId.getId()));
        t.assertExpectation(tenantIdAndCentricId);
        t.clearExpectations();

        view = t.readView(tenantIdAndCentricId, actorId, new ObjectId(viewClass, docId.getId()));
        Assert.assertNotNull(view);

        Assert.assertTrue(view.has("latest_ref_tagged"));

        JsonNode tagNode = view.get("latest_ref_tagged");

        Assert.assertNotNull(tagNode);

        Assert.assertFalse(tagNode.has("tagger"));

        System.out.println("View with leaf deleted:\n" + mapper.writeValueAsString(view));
    }

    @Test (dataProvider = "tasmoMaterializer", invocationCount = 1, singleThreaded = true)
    public void testDeleteRefWithMultipleSubRefs(TasmoMaterializerHarness t) throws Exception {
        String viewClassName = "ActivityView";
        Views views = TasmoModelFactory.modelToViews(
                viewClassName + "::path0::Activity.verbSubjectEventId",
                viewClassName + "::path1::Activity.verbSubject.ref.CommentVersion|CommentVersion.author.ref.User|User.firstName",
                viewClassName + "::path2::Activity.verbSubject.ref.CommentVersion|CommentVersion.activityParent.ref.Document|Document.subject");
        t.initModel(views);

        ObjectId author = t.write(EventBuilder.create(t.idProvider(), "User", tenantId, actorId)
                .set("firstName", "John")
                .set("lastName", "Doe")
                .build());

        ObjectId document = t.write(EventBuilder.create(t.idProvider(), "Document", tenantId, actorId)
                .set("subject", "Subject")
                .build());

        ObjectId verbSubject = t.write(EventBuilder.create(t.idProvider(), "CommentVersion", tenantId, actorId)
                .set("author", author.toStringForm())
                .set("activityParent", document.toStringForm())
                .build());

        ObjectId activity = t.write(EventBuilder.create(t.idProvider(), "Activity", tenantId, actorId)
                .set("verbSubject", verbSubject.toStringForm())
                .set("verbSubjectEventId", "12345")
                .build());

        ObjectNode view = t.readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, activity.getId()));
        System.out.println(mapper.writeValueAsString(view));
        System.out.flush();

        Assert.assertNotNull(view);
        Assert.assertNotNull(view.get("verbSubject"));

        // Delete the verb subject
        t.write(EventBuilder.update(verbSubject, tenantId, actorId).set(ReservedFields.DELETED, true).build());

        view = t.readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, activity.getId()));
        System.out.println(mapper.writeValueAsString(view));
        System.out.flush();

        Assert.assertNotNull(view);
        Assert.assertNull(view.get("verbSubject"));
    }

    @Test (dataProvider = "tasmoMaterializer", invocationCount = 1, singleThreaded = true)
    public void testThreeLevelsDeleteTailAndReLink(TasmoMaterializerHarness t) throws Exception {
        //LogManager.getRootLogger().setLevel(Level.TRACE);
        String viewClassName = "3Levels";
        String pathId = "path";
        Views views = TasmoModelFactory.modelToViews(
                viewClassName + "::" + pathId + "::CommentVersion.parent.ref.Comment|Comment.author.ref.User|User.firstName");
        t.initModel(views);

        ObjectId author = t.write(EventBuilder.create(t.idProvider(), "User", tenantId, actorId)
                .set("firstName", "John")
                .build());

        ObjectId comment = t.write(EventBuilder.create(t.idProvider(), "Comment", tenantId, actorId)
                .set("author", author)
                .build());

        ObjectId commentVersion = t.write(EventBuilder.create(t.idProvider(), "CommentVersion", tenantId, actorId)
                .set("parent", comment)
                .build());

        // commentVersion -(parent)-> comment -(author)-> author.firstName
        ObjectNode view = t.readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, commentVersion.getId()));
        t.addExpectation(commentVersion, viewClassName, pathId, new ObjectId[]{commentVersion, comment, author}, "firstName", "John");
        t.readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, commentVersion.getId()));
        t.assertExpectation(tenantIdAndCentricId);
        t.clearExpectations();
        System.out.println("view:" + view);
        Assert.assertNotNull(view);
        System.out.println("--------------------------------------------------------------------");

        t.write(EventBuilder.update(author, tenantId, actorId).set(ReservedFields.DELETED, true).build());

        t.addExpectation(commentVersion, viewClassName, pathId, new ObjectId[]{commentVersion, comment, author}, "firstName", null);
        t.assertExpectation(tenantIdAndCentricId);
        t.clearExpectations();
        view = t.readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, commentVersion.getId()));

        System.out.println("view:" + view);
        System.out.println("--------------------------------------------------------------------");
        Assert.assertNull(view);

        t.write(EventBuilder.update(comment, tenantId, actorId)
                .set("author", author)
                .build());

        t.addExpectation(commentVersion, viewClassName, pathId, new ObjectId[]{commentVersion, comment, author}, "firstName", null);
        t.assertExpectation(tenantIdAndCentricId);
        t.clearExpectations();
        view = t.readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, commentVersion.getId()));
        System.out.println("view:" + view);
        System.out.println("--------------------------------------------------------------------");
        Assert.assertNull(view);

    }

    @Test (dataProvider = "tasmoMaterializer", invocationCount = 1, singleThreaded = true)
    public void testThreeLevelsDeleteMiddleAndReLink(TasmoMaterializerHarness t) throws Exception {
        String viewClassName = "3Levels";
        String pathId = "path";
        Views views = TasmoModelFactory.modelToViews(
                viewClassName + "::" + pathId + "::CommentVersion.parent.ref.Comment|Comment.author.ref.User|User.firstName");
        t.initModel(views);

        ObjectId author = t.write(EventBuilder.create(t.idProvider(), "User", tenantId, actorId)
                .set("firstName", "John")
                .build());

        ObjectId comment = t.write(EventBuilder.create(t.idProvider(), "Comment", tenantId, actorId)
                .set("author", author)
                .build());

        ObjectId commentVersion = t.write(EventBuilder.create(t.idProvider(), "CommentVersion", tenantId, actorId)
                .set("parent", comment)
                .build());

        ObjectNode view = t.readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, commentVersion.getId()));
        t.addExpectation(commentVersion, viewClassName, pathId, new ObjectId[]{commentVersion, comment, author}, "firstName", "John");
        t.readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, commentVersion.getId()));
        t.assertExpectation(tenantIdAndCentricId);
        t.clearExpectations();

        Assert.assertNotNull(view);

        t.write(EventBuilder.update(comment, tenantId, actorId).set(ReservedFields.DELETED, true).build());

        t.addExpectation(commentVersion, viewClassName, pathId, new ObjectId[]{commentVersion, comment, author}, "firstName", null);
        t.assertExpectation(tenantIdAndCentricId);
        t.clearExpectations();
        view = t.readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, commentVersion.getId()));

        Assert.assertNull(view);

        t.write(EventBuilder.update(commentVersion, tenantId, actorId)
                .set("parent", comment)
                .build());

        //ref between comment and author is gone
        t.addExpectation(commentVersion, viewClassName, pathId, new ObjectId[]{commentVersion, comment, author}, "firstName", null);
        t.assertExpectation(tenantIdAndCentricId);
        t.clearExpectations();
        view = t.readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, commentVersion.getId()));

        Assert.assertNull(view);

    }

    @Test (dataProvider = "tasmoMaterializer", invocationCount = 1, singleThreaded = true)
    public void testThreeLevelsDeleteHeadAndReLink(TasmoMaterializerHarness t) throws Exception {
        String viewClassName = "3Levels";
        String pathId = "path";
        Views views = TasmoModelFactory.modelToViews(
                viewClassName + "::" + pathId + "::CommentVersion.parent.ref.Comment|Comment.author.ref.User|User.firstName");
        t.initModel(views);

        ObjectId author = t.write(EventBuilder.create(t.idProvider(), "User", tenantId, actorId)
                .set("firstName", "John")
                .build());

        ObjectId comment = t.write(EventBuilder.create(t.idProvider(), "Comment", tenantId, actorId)
                .set("author", author)
                .build());

        ObjectId commentVersion = t.write(EventBuilder.create(t.idProvider(), "CommentVersion", tenantId, actorId)
                .set("parent", comment)
                .build());

        ObjectNode view = t.readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, commentVersion.getId()));
        t.addExpectation(commentVersion, viewClassName, pathId, new ObjectId[]{commentVersion, comment, author}, "firstName", "John");
        t.readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, commentVersion.getId()));
        t.assertExpectation(tenantIdAndCentricId);
        t.clearExpectations();

        Assert.assertNotNull(view);

        t.write(EventBuilder.update(commentVersion, tenantId, actorId).set(ReservedFields.DELETED, true).build());

        view = t.readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, commentVersion.getId()));
        t.addExpectation(commentVersion, viewClassName, pathId, new ObjectId[]{commentVersion, comment, author}, "firstName", null);
        t.assertExpectation(tenantIdAndCentricId);
        t.clearExpectations();

        Assert.assertNull(view);

        t.write(EventBuilder.update(commentVersion, tenantId, actorId)
                .build());

        //ref between comment version and comment is still gone
        view = t.readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, commentVersion.getId()));
        t.addExpectation(commentVersion, viewClassName, pathId, new ObjectId[]{commentVersion, comment, author}, "firstName", null);
        t.assertExpectation(tenantIdAndCentricId);
        t.clearExpectations();

        Assert.assertNull(view);

    }
}
