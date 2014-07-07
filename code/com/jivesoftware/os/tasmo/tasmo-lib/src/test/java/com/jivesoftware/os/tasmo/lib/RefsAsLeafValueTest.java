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
import com.google.common.collect.Lists;
import com.jivesoftware.os.jive.utils.id.Id;
import com.jivesoftware.os.jive.utils.id.ObjectId;
import com.jivesoftware.os.tasmo.event.api.write.EventBuilder;
import com.jivesoftware.os.tasmo.model.Views;
import java.util.List;
import org.testng.annotations.Test;

/**
 *
 */
public class RefsAsLeafValueTest extends BaseTest {

    @Test (dataProvider = "tasmoMaterializer", invocationCount = 1, singleThreaded = true)
    public void testRefsAsLeafValue(TasmoMaterializerHarness t) throws Exception {
        String viewClassName = "Values";
        String viewFieldName = "refs";
        Views views = TasmoModelFactory.modelToViews(viewClassName + "::" + viewFieldName + "::User.refs_followed");
        t.initModel(views);
        List<ObjectId> followed = Lists.newArrayList();
        followed.add(new ObjectId("Place", new Id(2)));
        followed.add(new ObjectId("Place", new Id(3)));
        followed.add(new ObjectId("Place", new Id(4)));
        ObjectId user1 = t.write(EventBuilder.create(t.idProvider(), "User", tenantId, actorId).set("refs_followed", followed).build()); //2
        List<String> followedAsStrings = Lists.newArrayList();
        followedAsStrings.add(new ObjectId("Place", new Id(2)).toStringForm());
        followedAsStrings.add(new ObjectId("Place", new Id(3)).toStringForm());
        followedAsStrings.add(new ObjectId("Place", new Id(4)).toStringForm());
        t.addExpectation(user1, viewClassName, viewFieldName, new ObjectId[]{ user1 }, "refs_followed", followedAsStrings);
        t.readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, user1.getId()));
        t.assertExpectation(tenantIdAndCentricId);
        t.clearExpectations();
        ObjectNode view = t.readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, user1.getId()));
        System.out.println(mapper.writeValueAsString(view));
    }
}
