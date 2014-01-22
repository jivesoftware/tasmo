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
import org.testng.annotations.Test;

/**
 *
 */
public class BackRef2Test extends BaseTasmoTest {

    String VersionView = "VersionView";
    String versionedContentAuthorName = "versionAuthor";

    @Test
    public void testBackRef2() throws Exception {
        Expectations expectations =
            initModelPaths(VersionView + "::" + versionedContentAuthorName
            + "::VersionedContent.backRefs.Content.ref_versionedContent|Content.ref_originalAuthor.ref.User|User.firstName");
        ObjectId userId = write(EventBuilder.create(idProvider, "User", tenantId, actorId).set("firstName", "larry").build()); //2
        ObjectId contentId = write(EventBuilder.create(idProvider, "Content", tenantId, actorId).set("ref_originalAuthor", userId).build()); //4
        ObjectId versionId = write(EventBuilder.create(idProvider, "VersionedContent", tenantId, actorId).build()); //6
        write(EventBuilder.update(contentId, tenantId, actorId).set("ref_versionedContent", versionId).build()); //7
        //7
        expectations.addExpectation(versionId, VersionView, versionedContentAuthorName, new ObjectId[]{versionId, contentId, userId}, "firstName", "larry");
        expectations.assertExpectation(tenantIdAndCentricId);
        expectations.clear();
        ObjectNode view = readView(tenantIdAndCentricId, actorId, new ObjectId(VersionView, versionId.getId()));
        System.out.println(mapper.writeValueAsString(view));
    }
}
