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
public class CirculareRefTest extends BaseTest {

    @Test (dataProvider = "tasmoMaterializer", invocationCount = 1, singleThreaded = true)
    public void testCircularRef(TasmoMaterializerHarness t) throws Exception {
        String viewClassName = "RefToValues";
        String viewFieldName = "userInfo";
        Views views = TasmoModelFactory.modelToViews(viewClassName + "::" + viewFieldName + "::Content.ref_parent.ref.Content|Content.name");
        t.initModel(views);

        ObjectId parent1 = t.write(EventBuilder.create(t.idProvider(), "Content", tenantId, actorId).set("name", "ted").build());
        ObjectId content1 = t.write(EventBuilder.create(t.idProvider(), "Content", tenantId, actorId).set("ref_parent", parent1).build());

        t.addExpectation(content1, viewClassName, viewFieldName, new ObjectId[]{ content1, parent1 }, "name", "ted");


        ObjectNode view = t.readView(tenantId, actorId, new ObjectId(viewClassName, content1.getId()), Id.NULL);
        System.out.println("\nView:" + mapper.writeValueAsString(view) + "\n");
        t.assertExpectation(tenantIdAndCentricId);
        t.clearExpectations();
    }
}
