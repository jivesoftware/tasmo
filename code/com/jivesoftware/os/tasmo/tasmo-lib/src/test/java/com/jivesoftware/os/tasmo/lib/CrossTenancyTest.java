package com.jivesoftware.os.tasmo.lib;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.jivesoftware.os.jive.utils.id.Id;
import com.jivesoftware.os.jive.utils.id.ObjectId;
import com.jivesoftware.os.jive.utils.id.TenantId;
import com.jivesoftware.os.tasmo.event.api.write.EventBuilder;
import com.jivesoftware.os.tasmo.model.Views;
import java.util.Arrays;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * @author pete
 */
public class CrossTenancyTest extends BaseTest {

    @Test (dataProvider = "tasmoMaterializer", invocationCount = 1, singleThreaded = true)
    public void testCrossTenancyDoesNotMaterialize(TasmoMaterializerHarness t) throws Exception {
        String viewClassName = "Values";
        String viewFieldName = "userInfo";
         Views views = TasmoModelFactory.modelToViews(viewClassName + "::" + viewFieldName
            + "::User.refs_publications.refs.Content|Content.refs_authors.refs.User|User.first_name,last_name,email_address");
         t.initModel(views);

        TenantId otherTenant = new TenantId(("blah"));
        ObjectId user1 = t.write(EventBuilder.create(t.idProvider(), "User", otherTenant, actorId).set("first_name", "ted").build()); //1 - 2
        ObjectId content1 = t.write(EventBuilder.update(user1, tenantId, actorId).set("refs_authors", Arrays.asList(user1)).build()); //1 - 2

        t.addExpectation(user1, viewClassName, viewFieldName, new ObjectId[]{ user1, content1, user1 }, "first_name", null);
        t.assertExpectation(tenantIdAndCentricId);
        t.clearExpectations();

        ObjectNode view = t.readView(tenantId, actorId, new ObjectId(viewClassName, user1.getId()), Id.NULL);
        Assert.assertNull(view);

    }
}
