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
public class LatestBackRefsTest extends BaseTasmoTest {

    @Test
    public void testLatestBackRefs() throws Exception {
        String viewClassName = "BackRefs";
        String viewFieldName = "contentsUsers";
        Expectations expectations = initModelPaths(viewClassName + "::" + viewFieldName + "::Content.latest_backRef.User.refs_contents|User.userName");
        ObjectId contentId1 = write(EventBuilder.create(idProvider, "Content", tenantId, actorId).set("name", "content1").build());
        ObjectId contentId2 = write(EventBuilder.create(idProvider, "Content", tenantId, actorId).set("name", "content2").build());


        ObjectId otherAuthorId1 =
            write(EventBuilder.create(idProvider, "User", tenantId, actorId).set("userName", "jane")
            .set("refs_contents", Arrays.asList(contentId1, contentId2)).build());

        ObjectNode view = readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, contentId1.getId()));
        System.out.println(mapper.writeValueAsString(view));

        view = readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, contentId2.getId()));
        System.out.println(mapper.writeValueAsString(view));

        expectations.addExpectation(contentId1, viewClassName, viewFieldName, new ObjectId[]{contentId1, otherAuthorId1}, "userName", "jane");
        expectations.addExpectation(contentId2, viewClassName, viewFieldName, new ObjectId[]{contentId2, otherAuthorId1}, "userName", "jane");
        expectations.assertExpectation(tenantIdAndCentricId);
        expectations.clear();

        //--- now ted will link to the content
        System.out.println("now ted will link to the content..");

        ObjectId otherAuthorId2 =
            write(EventBuilder.create(idProvider, "User", tenantId, actorId).set("userName", "ted")
            .set("refs_contents", Arrays.asList(contentId2, contentId1)).build());

        view = readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, contentId1.getId()));
        System.out.println(mapper.writeValueAsString(view));

        view = readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, contentId2.getId()));
        System.out.println(mapper.writeValueAsString(view));

        expectations.addExpectation(contentId1, viewClassName, viewFieldName, new ObjectId[]{contentId1, otherAuthorId2}, "userName", "ted");
        expectations.addExpectation(contentId2, viewClassName, viewFieldName, new ObjectId[]{contentId2, otherAuthorId2}, "userName", "ted");
        expectations.addExpectation(contentId1, viewClassName, viewFieldName, new ObjectId[]{contentId1, otherAuthorId1}, "userName", null);
        expectations.addExpectation(contentId2, viewClassName, viewFieldName, new ObjectId[]{contentId2, otherAuthorId1}, "userName", null);

        expectations.assertExpectation(tenantIdAndCentricId);
        expectations.clear();


        //--- now john will link to the content
        System.out.println("now john will link to only content 2");
        ObjectId otherAuthorId3 = write(EventBuilder.create(idProvider, "User", tenantId, actorId).set("userName", "john")
            .set("refs_contents", Arrays.asList(contentId2)).build());

        view = readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, contentId1.getId()));
        System.out.println(mapper.writeValueAsString(view));

        view = readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, contentId2.getId()));
        System.out.println(mapper.writeValueAsString(view));

        expectations.addExpectation(contentId1, viewClassName, viewFieldName, new ObjectId[]{contentId1, otherAuthorId2}, "userName", "ted");
        expectations.addExpectation(contentId2, viewClassName, viewFieldName, new ObjectId[]{contentId2, otherAuthorId3}, "userName", "john");
        expectations.assertExpectation(tenantIdAndCentricId);
        expectations.clear();


    }

    @Test
    public void testLatestBackRefs2() throws Exception {
        String viewClassName = "BackRefs";
        String viewFieldName = "contentsUsers";
        Expectations expectations = initModelPaths(
            viewClassName + "::" + viewFieldName
            + "::Space.refs_docs.refs.Content|Content.latest_backRef.User.refs_contents|User.userName");


        ObjectId space1 = write(EventBuilder.create(idProvider, "Space", tenantId, actorId).set("name", "Space1").build());
        ObjectId space2 = write(EventBuilder.create(idProvider, "Space", tenantId, actorId).set("name", "Space2").build());


        ObjectId contentId1 = write(EventBuilder.create(idProvider, "Content", tenantId, actorId).set("name", "content1").build());
        ObjectId contentId2 = write(EventBuilder.create(idProvider, "Content", tenantId, actorId).set("name", "content2").build());


        ObjectId otherAuthorId1 =
            write(EventBuilder.create(idProvider, "User", tenantId, actorId).set("userName", "jane")
            .set("refs_contents", Arrays.asList(contentId1, contentId2)).build());

        write(EventBuilder.update(space1, tenantId, actorId)
            .set("refs_docs", Arrays.asList(contentId1)).build());

        write(EventBuilder.update(space2, tenantId, actorId)
            .set("refs_docs", Arrays.asList(contentId2)).build());

        ObjectNode view = readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, space1.getId()));
        System.out.println(mapper.writeValueAsString(view));

        view = readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, space2.getId()));
        System.out.println(mapper.writeValueAsString(view));

        expectations.addExpectation(space1, viewClassName, viewFieldName, new ObjectId[]{space1, contentId1, otherAuthorId1}, "userName", "jane");
        expectations.addExpectation(space2, viewClassName, viewFieldName, new ObjectId[]{space2, contentId2, otherAuthorId1}, "userName", "jane");
        expectations.assertExpectation(tenantIdAndCentricId);
        expectations.clear();

        //--- now ted will link to the content
        System.out.println("now ted will link to the content..");

        ObjectId otherAuthorId2 =
            write(EventBuilder.create(idProvider, "User", tenantId, actorId).set("userName", "ted")
            .set("refs_contents", Arrays.asList(contentId2, contentId1)).build());

        view = readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, space1.getId()));
        System.out.println(mapper.writeValueAsString(view));

        view = readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, space2.getId()));
        System.out.println(mapper.writeValueAsString(view));


        write(EventBuilder.update(space1, tenantId, actorId)
            .set("refs_docs", Arrays.asList(contentId2)).build());

        write(EventBuilder.update(space2, tenantId, actorId)
            .set("refs_docs", Arrays.asList(contentId1)).build());

        expectations.addExpectation(space2, viewClassName, viewFieldName, new ObjectId[]{space2, contentId1, otherAuthorId2}, "userName", "ted");
        expectations.addExpectation(space1, viewClassName, viewFieldName, new ObjectId[]{space1, contentId2, otherAuthorId2}, "userName", "ted");
        expectations.assertExpectation(tenantIdAndCentricId);
        expectations.clear();


        //--- now ted will link to the content
        System.out.println("now john will link to only content 2");
        ObjectId otherAuthorId3 = write(EventBuilder.create(idProvider, "User", tenantId, actorId).set("userName", "john")
            .set("refs_contents", Arrays.asList(contentId2)).build());

        view = readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, space1.getId()));
        System.out.println(mapper.writeValueAsString(view));

        view = readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, space2.getId()));
        System.out.println(mapper.writeValueAsString(view));

        expectations.addExpectation(space2, viewClassName, viewFieldName, new ObjectId[]{space2, contentId1, otherAuthorId2}, "userName", "ted");
        expectations.addExpectation(space1, viewClassName, viewFieldName, new ObjectId[]{space1, contentId2, otherAuthorId3}, "userName", "john");
        expectations.assertExpectation(tenantIdAndCentricId);
        expectations.clear();


    }
}
