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
import com.jivesoftware.os.tasmo.event.api.write.EventBuilder;
import com.jivesoftware.os.tasmo.id.ObjectId;
import java.util.Arrays;
import org.testng.annotations.Test;

/**
 *
 */
public class ReferenceBleedTest extends BaseTasmoTest {

    String ContentView = "ContentView";
    String ContainerView = "ContainerView";
    String containerName = "containerName";
    String tags = "tags";

    @Test
    public void testEnsureThereIsNoRefBleed() throws Exception {
        initModelPaths(ContentView + "::" + "contentSubject" + "::Content.refs_versionedContent.refs.VersionedContent|VersionedContent.subject", ContentView
            + "::" + containerName + "::Content.ref_parent.ref.Container|Container.name");
        ObjectId aversionedContentId =
            write(EventBuilder.create(idProvider, "VersionedContent", tenantId, actorId).set("subject", "a-subject").build());
        ObjectId bversionedContentId =
            write(EventBuilder.create(idProvider, "VersionedContent", tenantId, actorId).set("subject", "b-subject").build());
        ObjectId cversionedContentId =
            write(EventBuilder.create(idProvider, "VersionedContent", tenantId, actorId).set("subject", "c-subject").build());
        ObjectId dversionedContentId =
            write(EventBuilder.create(idProvider, "VersionedContent", tenantId, actorId).set("subject", "d-subject").build());
        ObjectId eversionedContentId =
            write(EventBuilder.create(idProvider, "VersionedContent", tenantId, actorId).set("subject", "e-subject").build());
        ObjectId acontainerId = write(EventBuilder.create(idProvider, "Container", tenantId, actorId).set("name", "a-container").build());
        write(EventBuilder.create(idProvider, "Content", tenantId, actorId).set("ref_parent", acontainerId)
            .set("refs_versionedContent", Arrays.asList(aversionedContentId)).build());
        write(EventBuilder.create(idProvider, "Content", tenantId, actorId).set("ref_parent", acontainerId)
            .set("refs_versionedContent", Arrays.asList(bversionedContentId)).build());
        write(EventBuilder.create(idProvider, "Content", tenantId, actorId).set("ref_parent", acontainerId)
            .set("refs_versionedContent", Arrays.asList(cversionedContentId)).build());
        write(EventBuilder.create(idProvider, "Content", tenantId, actorId).set("ref_parent", acontainerId)
            .set("refs_versionedContent", Arrays.asList(dversionedContentId)).build());
        ObjectId acontentId =
            write(EventBuilder.create(idProvider, "Content", tenantId, actorId).set("ref_parent", acontainerId)
            .set("refs_versionedContent", Arrays.asList(eversionedContentId)).build());
        ObjectId bcontainerId = write(EventBuilder.create(idProvider, "Container", tenantId, actorId).set("name", "b-container").build());
        write(EventBuilder.create(idProvider, "Content", tenantId, actorId).set("ref_parent", bcontainerId)
            .set("refs_versionedContent", Arrays.asList(bversionedContentId)).build());
        write(EventBuilder.create(idProvider, "Content", tenantId, actorId).set("ref_parent", bcontainerId)
            .set("refs_versionedContent", Arrays.asList(bversionedContentId)).build());
        ObjectId bcontentId =
            write(EventBuilder.create(idProvider, "Content", tenantId, actorId).set("ref_parent", bcontainerId)
            .set("refs_versionedContent", Arrays.asList(aversionedContentId)).build());
        //expectations.addExpectation(acontentId, ContentView, status, new ObjectId[]{acontentId}, "draft");
        //expectations.assertExpectation(tenantId);
        //expectations.clear();
        ObjectNode view = readView(tenantIdAndCentricId, actorId, new ObjectId(ContentView, acontentId.getId()));
        System.out.println(mapper.writeValueAsString(view));
        view = readView(tenantIdAndCentricId, actorId, new ObjectId(ContentView, bcontentId.getId()));
        System.out.println(mapper.writeValueAsString(view));
    }
}
