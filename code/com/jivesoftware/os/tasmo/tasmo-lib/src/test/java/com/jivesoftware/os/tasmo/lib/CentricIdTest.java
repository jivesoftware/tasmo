package com.jivesoftware.os.tasmo.lib;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.jivesoftware.os.tasmo.event.api.write.EventBuilder;
import com.jivesoftware.os.tasmo.id.Id;
import com.jivesoftware.os.tasmo.id.ObjectId;
import com.jivesoftware.os.tasmo.id.TenantIdAndCentricId;
import com.jivesoftware.os.tasmo.model.ViewBinding;
import java.util.List;
import org.testng.annotations.Test;

/**
 *
 */
public class CentricIdTest extends BaseTasmoTest {

    @Test
    public void testCentricAndNonCentricViews() throws Exception {
        String idCentricViewClassName = "IdCentricView";
        String idCentricViewFieldName = "idCentricUserInfo";
        String nonIdCentricViewClassName = "NonIdCentricView";
        String nonIdCentricViewFieldName = "nonIdCentricUserInfo";

        List<ViewBinding> viewBindings = parseModelPathStrings(true, idCentricViewClassName + "::" + idCentricViewFieldName + "::User.userName,age");
        viewBindings.addAll(parseModelPathStrings(false, nonIdCentricViewClassName + "::" + nonIdCentricViewFieldName + "::User.userName,age"));

        tenantIdAndCentricId = new TenantIdAndCentricId(tenantId, actorId);
        Expectations expectations = initModelPaths(viewBindings);
        ObjectId user1 = write(EventBuilder.create(idProvider, "User", tenantId, actorId).set("userName", "ted").build());

        ObjectNode view = readView(tenantIdAndCentricId, actorId, new ObjectId(idCentricViewClassName, user1.getId()));
        System.out.println("Centric View:" + mapper.writeValueAsString(view));
        // assert id centric
        expectations.addExpectation(user1, idCentricViewClassName, idCentricViewFieldName, new ObjectId[]{user1}, "userName", "ted");
        //expectations.assertExpectation(tenantIdAndCentricId);
        expectations.clear();

        // assert non id centric
        tenantIdAndCentricId = new TenantIdAndCentricId(tenantId, Id.NULL);
        view = readView(tenantIdAndCentricId, actorId, new ObjectId(nonIdCentricViewClassName, user1.getId()));
        System.out.println("System View:" + mapper.writeValueAsString(view));

        expectations.addExpectation(user1, nonIdCentricViewClassName, nonIdCentricViewFieldName, new ObjectId[]{user1}, "userName", "ted");
        //expectations.assertExpectation(tenantIdAndCentricId);
        expectations.clear();

    }

}
