package com.jivesoftware.os.tasmo.lib;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.jivesoftware.os.jive.utils.id.ObjectId;
import com.jivesoftware.os.jive.utils.id.TenantId;
import com.jivesoftware.os.tasmo.event.api.write.EventBuilder;
import java.util.Arrays;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * @author pete
 */
public class CrossTenancyTest extends BaseTasmoTest {

    @Test
    public void testCrossTenancyDoesNotMaterialize() throws Exception {
        String viewClassName = "Values";
        String viewFieldName = "userInfo";
        Expectations expectations =
            initModelPaths(viewClassName + "::" + viewFieldName
            + "::User.refs_publications.refs.Content|Content.refs_authors.refs.User|User.first_name,last_name,email_address");
        TenantId otherTenant = new TenantId(("blah"));
        ObjectId user1 = write(EventBuilder.create(idProvider, "User", otherTenant, actorId).set("first_name", "ted").build()); //1 - 2
        ObjectId content1 = write(EventBuilder.update(user1, tenantId, actorId).set("refs_authors", Arrays.asList(user1)).build()); //1 - 2

        expectations.addExpectation(user1, viewClassName, viewFieldName, new ObjectId[]{ user1, content1, user1 }, "first_name", null);
        expectations.assertExpectation(tenantIdAndCentricId);
        expectations.clear();

        ObjectNode view = readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName, user1.getId()));
        Assert.assertNull(view);

    }
}
