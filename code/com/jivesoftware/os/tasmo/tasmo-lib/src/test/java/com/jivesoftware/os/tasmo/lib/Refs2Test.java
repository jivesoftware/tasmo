/*
 * $Revision$
 * $Date$
 *
 * Copyright (C) 1999-$year$ Jive Software. All rights reserved.
 *
 * This software is the proprietary information of Jive Software. Use is subject to license terms.
 */
package com.jivesoftware.os.tasmo.lib;

import com.jivesoftware.os.tasmo.event.api.write.EventBuilder;
import com.jivesoftware.os.tasmo.id.ObjectId;
import java.util.Arrays;
import java.util.Collections;
import org.testng.annotations.Test;

/**
 *
 */
public class Refs2Test extends BaseTasmoTest {

    String ContentView = "ContentView";
    String tags = "tags";
    String status = "status";

    @Test
    public void testsRefs() throws Exception {
        Expectations expectations = initModelPaths(ContentView + "::" + tags + "::Content.refs_tags.refs.Tag|Tag.name");
        ObjectId contentId = write(EventBuilder.create(idProvider, "Content", tenantId, actorId).set("status", "hi").build());
        ObjectId tag1 = write(EventBuilder.create(idProvider, "Tag", tenantId, actorId).set("name", "tag1").build());
        ObjectId tag2 = write(EventBuilder.create(idProvider, "Tag", tenantId, actorId).set("name", "tag2").build());
        ObjectId tag3 = write(EventBuilder.create(idProvider, "Tag", tenantId, actorId).set("name", "tag3").build());
        ObjectId tag4 = write(EventBuilder.create(idProvider, "Tag", tenantId, actorId).set("name", "tag4").build());
        write(EventBuilder.update(contentId, tenantId, actorId).set("refs_tags", Arrays.asList(tag1, tag2, tag3)).build());
        expectations.addExpectation(contentId, ContentView, tags, new ObjectId[]{contentId, tag1}, "name", "tag1");
        expectations.addExpectation(contentId, ContentView, tags, new ObjectId[]{contentId, tag2}, "name", "tag2");
        expectations.addExpectation(contentId, ContentView, tags, new ObjectId[]{contentId, tag3}, "name", "tag3");
        expectations.addExpectation(contentId, ContentView, tags, new ObjectId[]{contentId, tag4}, "name", null);
        expectations.assertExpectation(tenantIdAndCentricId);
        expectations.clear();
        write(EventBuilder.update(tag1, tenantId, actorId).set("name", "tag1_prime").build());
        expectations.addExpectation(contentId, ContentView, tags, new ObjectId[]{contentId, tag1}, "name", "tag1_prime");
        expectations.addExpectation(contentId, ContentView, tags, new ObjectId[]{contentId, tag2}, "name", "tag2");
        expectations.addExpectation(contentId, ContentView, tags, new ObjectId[]{contentId, tag3}, "name", "tag3");
        expectations.addExpectation(contentId, ContentView, tags, new ObjectId[]{contentId, tag4}, "name", null);
        expectations.assertExpectation(tenantIdAndCentricId);
        expectations.clear();
        write(EventBuilder.update(contentId, tenantId, actorId).set("refs_tags", Arrays.asList(tag1, tag2)).build());
        expectations.addExpectation(contentId, ContentView, tags, new ObjectId[]{contentId, tag1}, "name", "tag1_prime");
        expectations.addExpectation(contentId, ContentView, tags, new ObjectId[]{contentId, tag2}, "name", "tag2");
        expectations.addExpectation(contentId, ContentView, tags, new ObjectId[]{contentId, tag3}, "name", null);
        expectations.addExpectation(contentId, ContentView, tags, new ObjectId[]{contentId, tag4}, "name", null);
        expectations.assertExpectation(tenantIdAndCentricId);
        expectations.clear();
        write(EventBuilder.update(contentId, tenantId, actorId).set("refs_tags", Arrays.asList(tag4)).build());
        expectations.addExpectation(contentId, ContentView, tags, new ObjectId[]{contentId, tag1}, "name", null);
        expectations.addExpectation(contentId, ContentView, tags, new ObjectId[]{contentId, tag2}, "name", null);
        expectations.addExpectation(contentId, ContentView, tags, new ObjectId[]{contentId, tag3}, "name", null);
        expectations.addExpectation(contentId, ContentView, tags, new ObjectId[]{contentId, tag4}, "name", "tag4");
        expectations.assertExpectation(tenantIdAndCentricId);
        expectations.clear();
        write(EventBuilder.update(contentId, tenantId, actorId).set("refs_tags", Collections.<ObjectId>emptyList()).build());
        expectations.addExpectation(contentId, ContentView, tags, new ObjectId[]{contentId, tag1}, "name", null);
        expectations.addExpectation(contentId, ContentView, tags, new ObjectId[]{contentId, tag2}, "name", null);
        expectations.addExpectation(contentId, ContentView, tags, new ObjectId[]{contentId, tag3}, "name", null);
        expectations.addExpectation(contentId, ContentView, tags, new ObjectId[]{contentId, tag4}, "name", null);
        expectations.assertExpectation(tenantIdAndCentricId);
        expectations.clear();
    }
}
