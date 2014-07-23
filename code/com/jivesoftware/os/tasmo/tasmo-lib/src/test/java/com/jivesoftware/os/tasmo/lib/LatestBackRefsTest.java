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
import com.jivesoftware.os.jive.utils.id.Id;
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
public class LatestBackRefsTest extends BaseTest {

    @Test (dataProvider = "tasmoMaterializer", invocationCount = 1, singleThreaded = true)
    public void testLatestBackRefs(TasmoMaterializerHarness t) throws Exception {
        String viewClassName = "BackRefs";
        String viewFieldName = "contentsUsers";
        Views views = TasmoModelFactory.modelToViews(viewClassName + "::" + viewFieldName + "::Content.latest_backRef.User.refs_contents|User.userName");
        t.initModel(views);

        ObjectId contentId1 = t.write(EventBuilder.create(t.idProvider(), "Content", tenantId, actorId).set("name", "content1").build());
        ObjectId contentId2 = t.write(EventBuilder.create(t.idProvider(), "Content", tenantId, actorId).set("name", "content2").build());

        ObjectId otherAuthorId1 =
            t.write(EventBuilder.create(t.idProvider(), "User", tenantId, actorId).set("userName", "jane")
                .set("refs_contents", Arrays.asList(contentId1, contentId2)).build());

        ObjectNode view = t.readView(tenantId, actorId, new ObjectId(viewClassName, contentId1.getId()), Id.NULL);
        System.out.println(mapper.writeValueAsString(view));

        view = t.readView(tenantId, actorId, new ObjectId(viewClassName, contentId2.getId()), Id.NULL);
        System.out.println(mapper.writeValueAsString(view));

        t.addExpectation(contentId1, viewClassName, viewFieldName, new ObjectId[]{ contentId1, otherAuthorId1 }, "userName", "jane");
        t.addExpectation(contentId2, viewClassName, viewFieldName, new ObjectId[]{ contentId2, otherAuthorId1 }, "userName", "jane");
        t.assertExpectation(tenantIdAndCentricId);
        t.clearExpectations();

        //--- now ted will link to the content
        System.out.println("now ted will link to the content..");

        ObjectId otherAuthorId2 =
            t.write(EventBuilder.create(t.idProvider(), "User", tenantId, actorId).set("userName", "ted")
                .set("refs_contents", Arrays.asList(contentId2, contentId1)).build());

        view = t.readView(tenantId, actorId, new ObjectId(viewClassName, contentId1.getId()), Id.NULL);
        System.out.println(mapper.writeValueAsString(view));

        view = t.readView(tenantId, actorId, new ObjectId(viewClassName, contentId2.getId()), Id.NULL);
        System.out.println(mapper.writeValueAsString(view));

        t.addExpectation(contentId1, viewClassName, viewFieldName, new ObjectId[]{ contentId1, otherAuthorId2 }, "userName", "ted");
        t.addExpectation(contentId2, viewClassName, viewFieldName, new ObjectId[]{ contentId2, otherAuthorId2 }, "userName", "ted");
        //we don't actually clean up the old latest anymore
        t.addExpectation(contentId1, viewClassName, viewFieldName, new ObjectId[]{ contentId1, otherAuthorId1 }, "userName", "jane");
        t.addExpectation(contentId2, viewClassName, viewFieldName, new ObjectId[]{ contentId2, otherAuthorId1 }, "userName", "jane");

        t.assertExpectation(tenantIdAndCentricId);
        t.clearExpectations();

        //--- now john will link to the content
        System.out.println("now john will link to only content 2");
        ObjectId otherAuthorId3 = t.write(EventBuilder.create(t.idProvider(), "User", tenantId, actorId).set("userName", "john")
            .set("refs_contents", Arrays.asList(contentId2)).build());

        view = t.readView(tenantId, actorId, new ObjectId(viewClassName, contentId1.getId()), Id.NULL);
        System.out.println(mapper.writeValueAsString(view));

        view = t.readView(tenantId, actorId, new ObjectId(viewClassName, contentId2.getId()), Id.NULL);
        System.out.println(mapper.writeValueAsString(view));

        t.addExpectation(contentId1, viewClassName, viewFieldName, new ObjectId[]{ contentId1, otherAuthorId2 }, "userName", "ted");
        t.addExpectation(contentId2, viewClassName, viewFieldName, new ObjectId[]{ contentId2, otherAuthorId3 }, "userName", "john");
        t.assertExpectation(tenantIdAndCentricId);
        t.clearExpectations();

    }

    @Test (dataProvider = "tasmoMaterializer", invocationCount = 1, singleThreaded = true)
    public void testLatestBackRefs2(TasmoMaterializerHarness t) throws Exception {
        String viewClassName = "BackRefs";
        String viewFieldName = "contentsUsers";
        Views views = TasmoModelFactory.modelToViews(
            viewClassName + "::" + viewFieldName
            + "::Space.refs_docs.refs.Content|Content.latest_backRef.User.refs_contents|User.userName");
        t.initModel(views);

        ObjectId space1 = t.write(EventBuilder.create(t.idProvider(), "Space", tenantId, actorId).set("name", "Space1").build());
        ObjectId space2 = t.write(EventBuilder.create(t.idProvider(), "Space", tenantId, actorId).set("name", "Space2").build());

        ObjectId contentId1 = t.write(EventBuilder.create(t.idProvider(), "Content", tenantId, actorId).set("name", "content1").build());
        ObjectId contentId2 = t.write(EventBuilder.create(t.idProvider(), "Content", tenantId, actorId).set("name", "content2").build());

        ObjectId otherAuthorId1 =
            t.write(EventBuilder.create(t.idProvider(), "User", tenantId, actorId).set("userName", "jane")
                .set("refs_contents", Arrays.asList(contentId1, contentId2)).build());

        t.write(EventBuilder.update(space1, tenantId, actorId)
            .set("refs_docs", Arrays.asList(contentId1)).build());

        t.write(EventBuilder.update(space2, tenantId, actorId)
            .set("refs_docs", Arrays.asList(contentId2)).build());

        ObjectNode view = t.readView(tenantId, actorId, new ObjectId(viewClassName, space1.getId()), Id.NULL);
        System.out.println(mapper.writeValueAsString(view));

        view = t.readView(tenantId, actorId, new ObjectId(viewClassName, space2.getId()), Id.NULL);
        System.out.println(mapper.writeValueAsString(view));

        t.addExpectation(space1, viewClassName, viewFieldName, new ObjectId[]{ space1, contentId1, otherAuthorId1 }, "userName", "jane");
        t.addExpectation(space2, viewClassName, viewFieldName, new ObjectId[]{ space2, contentId2, otherAuthorId1 }, "userName", "jane");
        t.assertExpectation(tenantIdAndCentricId);
        t.clearExpectations();

        //--- now ted will link to the content
        System.out.println("now ted will link to the content..");

        ObjectId otherAuthorId2 =
            t.write(EventBuilder.create(t.idProvider(), "User", tenantId, actorId).set("userName", "ted")
                .set("refs_contents", Arrays.asList(contentId2, contentId1)).build());

        view = t.readView(tenantId, actorId, new ObjectId(viewClassName, space1.getId()), Id.NULL);
        System.out.println(mapper.writeValueAsString(view));

        view = t.readView(tenantId, actorId, new ObjectId(viewClassName, space2.getId()), Id.NULL);
        System.out.println(mapper.writeValueAsString(view));

        t.write(EventBuilder.update(space1, tenantId, actorId)
            .set("refs_docs", Arrays.asList(contentId2)).build());

        t.write(EventBuilder.update(space2, tenantId, actorId)
            .set("refs_docs", Arrays.asList(contentId1)).build());

        t.addExpectation(space2, viewClassName, viewFieldName, new ObjectId[]{ space2, contentId1, otherAuthorId2 }, "userName", "ted");
        t.addExpectation(space1, viewClassName, viewFieldName, new ObjectId[]{ space1, contentId2, otherAuthorId2 }, "userName", "ted");
        t.readView(tenantId, actorId, new ObjectId(viewClassName, space2.getId()), Id.NULL);
        t.readView(tenantId, actorId, new ObjectId(viewClassName, space1.getId()), Id.NULL);
        t.assertExpectation(tenantIdAndCentricId);
        t.clearExpectations();

        //--- now ted will link to the content
        System.out.println("now john will link to only content 2");
        ObjectId otherAuthorId3 = t.write(EventBuilder.create(t.idProvider(), "User", tenantId, actorId).set("userName", "john")
            .set("refs_contents", Arrays.asList(contentId2)).build());

        view = t.readView(tenantId, actorId, new ObjectId(viewClassName, space1.getId()), Id.NULL);
        System.out.println(mapper.writeValueAsString(view));

        view = t.readView(tenantId, actorId, new ObjectId(viewClassName, space2.getId()), Id.NULL);
        System.out.println(mapper.writeValueAsString(view));

        t.addExpectation(space2, viewClassName, viewFieldName, new ObjectId[]{ space2, contentId1, otherAuthorId2 }, "userName", "ted");
        t.addExpectation(space1, viewClassName, viewFieldName, new ObjectId[]{ space1, contentId2, otherAuthorId3 }, "userName", "john");
        t.assertExpectation(tenantIdAndCentricId);
        t.clearExpectations();

    }

    @Test (dataProvider = "tasmoMaterializer", invocationCount = 1, singleThreaded = true)
    public void changesDownstream(TasmoMaterializerHarness t) throws Exception {
        String viewClassName = "BackRefs";
        String viewFieldName = "latestContentAuthor";
        Views views = TasmoModelFactory.modelToViews(viewClassName + "::" + viewFieldName
            + "::Document.latest_backRef.DocumentVersion.versionParent|DocumentVersion.author.ref.User|User.userName");
        t.initModel(views);

        ObjectId user1 = t.write(EventBuilder.create(t.idProvider(), "User", tenantId, actorId).set("userName", "Paul").build());
        ObjectId document = t.write(EventBuilder.create(t.idProvider(), "Document", tenantId, actorId).build());
        ObjectId version1 = t.write(EventBuilder.create(t.idProvider(), "DocumentVersion", tenantId, actorId)
            .set("versionParent", document).set("author", user1).build());
        ObjectId user2 = t.write(EventBuilder.create(t.idProvider(), "User", tenantId, actorId).set("userName", "John").build());
        ObjectId version2 = t.write(EventBuilder.create(t.idProvider(), "DocumentVersion", tenantId, actorId)
            .set("versionParent", document).set("author", user2).build());

        ObjectNode view1 = t.readView(tenantId, actorId, new ObjectId(viewClassName, document.getId()), Id.NULL);
        System.out.println(mapper.writeValueAsString(view1));
        JsonNode latest_versionParent = view1.get("latest_versionParent");
        Assert.assertNotNull(latest_versionParent, "latest_versionParent");
        JsonNode author = latest_versionParent.get("author");
        Assert.assertNotNull(author, "author");
        JsonNode userName = author.get("userName");
        Assert.assertNotNull(userName, "userName");
        Assert.assertEquals(userName.asText(), "John");

        t.write(EventBuilder.update(user1, tenantId, actorId).set("userName", "Sir Paul").build());

        ObjectNode view2 = t.readView(tenantId, actorId, new ObjectId(viewClassName, document.getId()), Id.NULL);
        System.out.println(mapper.writeValueAsString(view2));
        Assert.assertEquals(view2, view1);
    }

    @Test (dataProvider = "tasmoMaterializer", invocationCount = 1, singleThreaded = true)
    public void testBackRefInDeleteFlow(TasmoMaterializerHarness t) throws Exception {

        String viewClassName1 = "View1";
        String viewClassName2 = "View2";
        Views views = TasmoModelFactory.modelToViews(
            viewClassName1 + "::field1::Doc.latest_backRef.DocV.versionParent|DocV.binary.ref.SubDoc|SubDoc.data",
            viewClassName1 + "::field2::Doc.latest_backRef.DocV.versionParent|DocV.versionName",
            viewClassName1 + "::field3::Doc.name",
            viewClassName1 + "::field4::Doc.num",
            viewClassName2 + "::field1::DocV.binary.ref.SubDoc|SubDoc.data",
            viewClassName2 + "::field2::DocV.versionName"
        );
        t.initModel(views);

        ObjectId subDoc1 = t.write(EventBuilder.create(t.idProvider(), "SubDoc", tenantId, actorId).set("data", "foo").build());
        ObjectId doc1 = t.write(EventBuilder.create(t.idProvider(), "Doc", tenantId, actorId).set("name", "doc1").set("num", "1").build());
        ObjectId docV1 = t.write(EventBuilder.create(t.idProvider(), "DocV", tenantId, actorId)
            .set("versionParent", doc1).set("binary", subDoc1).set("versionName", "v1").build());

        ObjectNode view1 = t.readView(tenantId, actorId, new ObjectId(viewClassName1, doc1.getId()), Id.NULL);
        System.out.println("view1:" + mapper.writeValueAsString(view1));

        ObjectNode view2 = t.readView(tenantId, actorId, new ObjectId(viewClassName2, docV1.getId()), Id.NULL);
        System.out.println("view2:" + mapper.writeValueAsString(view2));

        t.write(EventBuilder.update(subDoc1, tenantId, actorId).set(ReservedFields.DELETED, true).build());

        System.out.println("--- Deleted ---");

        view1 = t.readView(tenantId, actorId, new ObjectId(viewClassName1, doc1.getId()), Id.NULL);
        System.out.println("view1:" + mapper.writeValueAsString(view1));

        view2 = t.readView(tenantId, actorId, new ObjectId(viewClassName2, docV1.getId()), Id.NULL);
        System.out.println("view2:" + mapper.writeValueAsString(view2));

    }
}
