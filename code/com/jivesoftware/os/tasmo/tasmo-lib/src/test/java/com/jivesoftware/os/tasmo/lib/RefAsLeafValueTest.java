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
import com.jivesoftware.os.jive.utils.id.Id;
import com.jivesoftware.os.jive.utils.id.ObjectId;
import com.jivesoftware.os.tasmo.event.api.write.EventBuilder;
import com.jivesoftware.os.tasmo.model.Views;
import org.testng.annotations.Test;

/**
 *
 */
public class RefAsLeafValueTest extends BaseTest {

    @Test (dataProvider = "tasmoMaterializer", invocationCount = 1, singleThreaded = true)
    public void testRefAsLeafValue(TasmoMaterializerHarness t) throws Exception {
        String viewClassName = "Values";
        String viewFieldName = "ref";
        Views views = TasmoModelFactory.modelToViews(viewClassName + "::" + viewFieldName + "::User.ref_followed");
        t.initModel(views);
        ObjectId user1 = t.write(EventBuilder.create(t.idProvider(), "User", tenantId, actorId)
            .set("ref_followed", new ObjectId("Place", new Id(2))).build()); //2


        t.addExpectation(user1, viewClassName, viewFieldName, new ObjectId[]{ user1 }, "ref_followed",
            "Place_" + new Id(2).toStringForm());

        ObjectNode view = t.readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, user1.getId()));
        System.out.println(mapper.writeValueAsString(view));

        t.assertExpectation(tenantIdAndCentricId);
        t.clearExpectations();

    }
}
