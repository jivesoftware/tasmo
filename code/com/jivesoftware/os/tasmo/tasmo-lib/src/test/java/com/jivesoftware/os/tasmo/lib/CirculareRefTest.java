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
import org.testng.annotations.Test;

/**
 *
 */
public class CirculareRefTest extends BaseTasmoTest {

    @Test
    public void testCircularRef() throws Exception {
        String viewClassName = "RefToValues";
        String viewFieldName = "userInfo";
        Expectations expectations = initModelPaths(viewClassName + "::" + viewFieldName + "::Content.ref_parent.ref.Content|Content.name");
        ObjectId parent1 = write(EventBuilder.create(idProvider, "Content", tenantId, actorId).set("name", "ted").build());
        ObjectId content1 = write(EventBuilder.create(idProvider, "Content", tenantId, actorId).set("ref_parent", parent1).build());
        ObjectNode view = readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, parent1.getId()));
        System.out.println("\nView:" + mapper.writeValueAsString(view) + "\n");

        expectations.addExpectation(content1, viewClassName, viewFieldName, new ObjectId[]{ content1, parent1 }, "name", "ted");
        expectations.assertExpectation(tenantIdAndCentricId);
        expectations.clear();
    }
}
