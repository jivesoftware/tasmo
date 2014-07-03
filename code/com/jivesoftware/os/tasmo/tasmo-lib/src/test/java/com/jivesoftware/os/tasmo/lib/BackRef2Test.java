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
import org.testng.annotations.Test;

/**
 *
 */
public class BackRef2Test extends BaseTest {

    @Test (dataProvider = "tasmoMaterializer", invocationCount = 1, singleThreaded = true)
    public void testBackRef2(TasmoMaterializerHarness t) throws Exception {
        Views views = TasmoModelFactory.modelToViews("VersionView::versionAuthor"
                + "::VersionedContent.backRefs.Content.ref_versionedContent|Content.ref_originalAuthor.ref.User|User.firstName");
        t.initModel(views);

        ObjectId userId = t.write(EventBuilder.create(t.idProvider(), "User", tenantId, actorId).set("firstName", "larry").build()); //2
        ObjectId contentId = t.write(EventBuilder.create(t.idProvider(), "Content", tenantId, actorId).set("ref_originalAuthor", userId).build()); //4
        ObjectId versionId = t.write(EventBuilder.create(t.idProvider(), "VersionedContent", tenantId, actorId).build()); //6
        t.write(EventBuilder.update(contentId, tenantId, actorId).set("ref_versionedContent", versionId).build()); //7
        // User2, Content4 -> User2, Versioned6, Content4 -> Versions6
        //7

        t.addExpectation(versionId, "VersionView", "versionAuthor", new ObjectId[]{versionId, contentId, userId}, "firstName", "larry");

        ObjectNode view = t.readView(tenantIdAndCentricId, actorId, new ObjectId("VersionView", versionId.getId()));
        t.assertExpectation(tenantIdAndCentricId);
        t.clearExpectations();
    }
}
