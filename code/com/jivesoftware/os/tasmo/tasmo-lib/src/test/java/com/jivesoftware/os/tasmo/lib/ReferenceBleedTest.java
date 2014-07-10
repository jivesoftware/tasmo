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
import com.jivesoftware.os.tasmo.model.Views;
import java.util.Arrays;
import org.testng.annotations.Test;

/**
 *
 */
public class ReferenceBleedTest extends BaseTest {

    String ContentView = "ContentView";
    String ContainerView = "ContainerView";
    String containerName = "containerName";
    String tags = "tags";

    @Test (dataProvider = "tasmoMaterializer", invocationCount = 1, singleThreaded = true)
    public void testEnsureThereIsNoRefBleed(TasmoMaterializerHarness t) throws Exception {
        Views views = TasmoModelFactory.modelToViews(
            ContentView + "::" + "contentSubject" + "::Content.refs_versionedContent.refs.VersionedContent|VersionedContent.subject", ContentView
            + "::" + containerName + "::Content.ref_parent.ref.Container|Container.name");
        t.initModel(views);

        ObjectId aversionedContentId =
            t.write(EventBuilder.create(t.idProvider(), "VersionedContent", tenantId, actorId).set("subject", "a-subject").build());
        ObjectId bversionedContentId =
            t.write(EventBuilder.create(t.idProvider(), "VersionedContent", tenantId, actorId).set("subject", "b-subject").build());
        ObjectId cversionedContentId =
            t.write(EventBuilder.create(t.idProvider(), "VersionedContent", tenantId, actorId).set("subject", "c-subject").build());
        ObjectId dversionedContentId =
            t.write(EventBuilder.create(t.idProvider(), "VersionedContent", tenantId, actorId).set("subject", "d-subject").build());
        ObjectId eversionedContentId =
            t.write(EventBuilder.create(t.idProvider(), "VersionedContent", tenantId, actorId).set("subject", "e-subject").build());
        ObjectId acontainerId = t.write(EventBuilder.create(t.idProvider(), "Container", tenantId, actorId).set("name", "a-container").build());
        t.write(EventBuilder.create(t.idProvider(), "Content", tenantId, actorId).set("ref_parent", acontainerId)
            .set("refs_versionedContent", Arrays.asList(aversionedContentId)).build());
        t.write(EventBuilder.create(t.idProvider(), "Content", tenantId, actorId).set("ref_parent", acontainerId)
            .set("refs_versionedContent", Arrays.asList(bversionedContentId)).build());
        t.write(EventBuilder.create(t.idProvider(), "Content", tenantId, actorId).set("ref_parent", acontainerId)
            .set("refs_versionedContent", Arrays.asList(cversionedContentId)).build());
        t.write(EventBuilder.create(t.idProvider(), "Content", tenantId, actorId).set("ref_parent", acontainerId)
            .set("refs_versionedContent", Arrays.asList(dversionedContentId)).build());
        ObjectId acontentId =
            t.write(EventBuilder.create(t.idProvider(), "Content", tenantId, actorId).set("ref_parent", acontainerId)
                .set("refs_versionedContent", Arrays.asList(eversionedContentId)).build());
        ObjectId bcontainerId = t.write(EventBuilder.create(t.idProvider(), "Container", tenantId, actorId).set("name", "b-container").build());
        t.write(EventBuilder.create(t.idProvider(), "Content", tenantId, actorId).set("ref_parent", bcontainerId)
            .set("refs_versionedContent", Arrays.asList(bversionedContentId)).build());
        t.write(EventBuilder.create(t.idProvider(), "Content", tenantId, actorId).set("ref_parent", bcontainerId)
            .set("refs_versionedContent", Arrays.asList(bversionedContentId)).build());
        ObjectId bcontentId =
            t.write(EventBuilder.create(t.idProvider(), "Content", tenantId, actorId).set("ref_parent", bcontainerId)
                .set("refs_versionedContent", Arrays.asList(aversionedContentId)).build());
        //t.addExpectation(acontentId, ContentView, status, new ObjectId[]{acontentId}, "draft");
        //t.assertExpectation(tenantId);
        //t.clearExpectations();
        ObjectNode view = t.readView(tenantIdAndCentricId, actorId, new ObjectId(ContentView, acontentId.getId()));
        System.out.println(mapper.writeValueAsString(view));
        view = t.readView(tenantIdAndCentricId, actorId, new ObjectId(ContentView, bcontentId.getId()));
        System.out.println(mapper.writeValueAsString(view));
    }
}
