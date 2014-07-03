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
import com.jivesoftware.os.tasmo.event.api.ReservedFields;
import com.jivesoftware.os.tasmo.event.api.write.EventBuilder;
import com.jivesoftware.os.tasmo.model.Views;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 *
 */
public class DeletesWithAdditionalFieldsTest extends BaseTest {

    @Test (dataProvider = "tasmoMaterializer", invocationCount = 1, singleThreaded = true)
    public void testDeletes(TasmoMaterializerHarness t) throws Exception {
        String viewClassName = "Values";
        String viewFieldName = "userInfo";
        Views views = TasmoModelFactory.modelToViews(viewClassName + "::" + viewFieldName + "::User.userName,age");
        t.initModel(views);

        ObjectId user1 = t.write(EventBuilder.create(t.idProvider(), "User", tenantId, actorId).set("userName", "ted").build());

        t.addExpectation(user1, viewClassName, viewFieldName, new ObjectId[]{user1}, "userName", "ted");
        t.readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, user1.getId()));
        t.assertExpectation(tenantIdAndCentricId);
        t.clearExpectations();

        ObjectNode view = t.readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, user1.getId()));
        Assert.assertNotNull(view);

        t.write(EventBuilder.update(user1, tenantId, actorId).set(ReservedFields.DELETED, true).set("userName", "frank").build());

        view = t.readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, user1.getId()));
        Assert.assertNull(view);

    }

}
