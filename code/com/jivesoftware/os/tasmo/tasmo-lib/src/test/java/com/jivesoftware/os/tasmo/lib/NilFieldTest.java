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
import org.testng.annotations.Test;

/**
 *
 */
public class NilFieldTest extends BaseTest {

    private static final String NIL = ReservedFields.NIL_FIELD;

    /**
     * Ensure nil field value is stored and retrieved properly.
     */
    @Test (dataProvider = "tasmoMaterializer", invocationCount = 1, singleThreaded = true)
    public void testRootHasNilFieldOnly(TasmoMaterializerHarness t) throws Exception {
        String viewClassName = "NilOnly";
        String viewFieldName = "nilField";
        Views views = TasmoModelFactory.modelToViews(viewClassName + "::" + viewFieldName + "::Content." + NIL);
        t.initModel(views);
        ObjectId content1 = t.write(EventBuilder.create(t.idProvider(), "Content", tenantId, actorId).build());
        t.addExpectation(content1, viewClassName, viewFieldName, new ObjectId[]{ content1 }, NIL, 0);
        t.assertExpectation(tenantIdAndCentricId);
        t.clearExpectations();
        ObjectNode view = t.readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, content1.getId()));
        System.out.println(mapper.writeValueAsString(view));
    }

}
