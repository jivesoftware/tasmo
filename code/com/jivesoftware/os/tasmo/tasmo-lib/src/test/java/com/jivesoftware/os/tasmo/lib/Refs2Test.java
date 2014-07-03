/*
 * $Revision$
 * $Date$
 *
 * Copyright (C) 1999-$year$ Jive Software. All rights reserved.
 *
 * This software is the proprietary information of Jive Software. Use is subject to license terms.
 */
package com.jivesoftware.os.tasmo.lib;

import com.jivesoftware.os.jive.utils.id.ObjectId;
import com.jivesoftware.os.tasmo.event.api.write.EventBuilder;
import com.jivesoftware.os.tasmo.model.Views;
import java.util.Arrays;
import java.util.Collections;
import org.testng.annotations.Test;

/**
 *
 */
public class Refs2Test extends BaseTest {

    String ContentView = "ContentView";
    String tags = "tags";
    String status = "status";

    @Test (dataProvider = "tasmoMaterializer", invocationCount = 1, singleThreaded = true)
    public void testsRefs(TasmoMaterializerHarness t) throws Exception {
        Views views = TasmoModelFactory.modelToViews(ContentView + "::" + tags + "::Content.refs_tags.refs.Tag|Tag.name");
        t.initModel(views);
        ObjectId contentId = t.write(EventBuilder.create(t.idProvider(), "Content", tenantId, actorId).set("status", "hi").build());
        ObjectId tag1 = t.write(EventBuilder.create(t.idProvider(), "Tag", tenantId, actorId).set("name", "tag1").build());
        ObjectId tag2 = t.write(EventBuilder.create(t.idProvider(), "Tag", tenantId, actorId).set("name", "tag2").build());
        ObjectId tag3 = t.write(EventBuilder.create(t.idProvider(), "Tag", tenantId, actorId).set("name", "tag3").build());
        ObjectId tag4 = t.write(EventBuilder.create(t.idProvider(), "Tag", tenantId, actorId).set("name", "tag4").build());
        t.write(EventBuilder.update(contentId, tenantId, actorId).set("refs_tags", Arrays.asList(tag1, tag2, tag3)).build());
        t.addExpectation(contentId, ContentView, tags, new ObjectId[]{contentId, tag1}, "name", "tag1");
        t.addExpectation(contentId, ContentView, tags, new ObjectId[]{contentId, tag2}, "name", "tag2");
        t.addExpectation(contentId, ContentView, tags, new ObjectId[]{contentId, tag3}, "name", "tag3");
        t.addExpectation(contentId, ContentView, tags, new ObjectId[]{contentId, tag4}, "name", null);
        t.assertExpectation(tenantIdAndCentricId);
        t.clearExpectations();
        t.write(EventBuilder.update(tag1, tenantId, actorId).set("name", "tag1_prime").build());
        t.addExpectation(contentId, ContentView, tags, new ObjectId[]{contentId, tag1}, "name", "tag1_prime");
        t.addExpectation(contentId, ContentView, tags, new ObjectId[]{contentId, tag2}, "name", "tag2");
        t.addExpectation(contentId, ContentView, tags, new ObjectId[]{contentId, tag3}, "name", "tag3");
        t.addExpectation(contentId, ContentView, tags, new ObjectId[]{contentId, tag4}, "name", null);
        t.assertExpectation(tenantIdAndCentricId);
        t.clearExpectations();
        t.write(EventBuilder.update(contentId, tenantId, actorId).set("refs_tags", Arrays.asList(tag1, tag2)).build());
        t.addExpectation(contentId, ContentView, tags, new ObjectId[]{contentId, tag1}, "name", "tag1_prime");
        t.addExpectation(contentId, ContentView, tags, new ObjectId[]{contentId, tag2}, "name", "tag2");
        t.addExpectation(contentId, ContentView, tags, new ObjectId[]{contentId, tag3}, "name", null);
        t.addExpectation(contentId, ContentView, tags, new ObjectId[]{contentId, tag4}, "name", null);
        t.assertExpectation(tenantIdAndCentricId);
        t.clearExpectations();
        t.write(EventBuilder.update(contentId, tenantId, actorId).set("refs_tags", Arrays.asList(tag4)).build());
        t.addExpectation(contentId, ContentView, tags, new ObjectId[]{contentId, tag1}, "name", null);
        t.addExpectation(contentId, ContentView, tags, new ObjectId[]{contentId, tag2}, "name", null);
        t.addExpectation(contentId, ContentView, tags, new ObjectId[]{contentId, tag3}, "name", null);
        t.addExpectation(contentId, ContentView, tags, new ObjectId[]{contentId, tag4}, "name", "tag4");
        t.assertExpectation(tenantIdAndCentricId);
        t.clearExpectations();
        t.write(EventBuilder.update(contentId, tenantId, actorId).set("refs_tags", Collections.<ObjectId>emptyList()).build());
        t.addExpectation(contentId, ContentView, tags, new ObjectId[]{contentId, tag1}, "name", null);
        t.addExpectation(contentId, ContentView, tags, new ObjectId[]{contentId, tag2}, "name", null);
        t.addExpectation(contentId, ContentView, tags, new ObjectId[]{contentId, tag3}, "name", null);
        t.addExpectation(contentId, ContentView, tags, new ObjectId[]{contentId, tag4}, "name", null);
        t.assertExpectation(tenantIdAndCentricId);
        t.clearExpectations();
    }
}
