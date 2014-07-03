package com.jivesoftware.os.tasmo.lib;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.jivesoftware.os.jive.utils.id.Id;
import com.jivesoftware.os.jive.utils.id.ObjectId;
import com.jivesoftware.os.jive.utils.id.TenantIdAndCentricId;
import com.jivesoftware.os.tasmo.event.api.write.EventBuilder;
import com.jivesoftware.os.tasmo.model.ViewBinding;
import com.jivesoftware.os.tasmo.model.Views;
import java.util.List;
import org.testng.annotations.Test;


/**
 *
 */
public class CentricIdTest extends BaseTest {

    @Test (dataProvider = "tasmoMaterializer", invocationCount = 1, singleThreaded = true)
    public void testCentricAndNonCentricViews(TasmoMaterializerHarness t) throws Exception {
        String idCentricViewClassName = "IdCentricView";
        String idCentricViewFieldName = "idCentricUserInfo";
        String nonIdCentricViewClassName = "NonIdCentricView";
        String nonIdCentricViewFieldName = "nonIdCentricUserInfo";

        List<ViewBinding> viewBindings = TasmoModelFactory
            .parseModelPathStrings(true, idCentricViewClassName + "::" + idCentricViewFieldName + "::User.userName,age");
        viewBindings.addAll(TasmoModelFactory
            .parseModelPathStrings(false, nonIdCentricViewClassName + "::" + nonIdCentricViewFieldName + "::User.userName,age"));



        tenantIdAndCentricId = new TenantIdAndCentricId(tenantId, actorId);
        Views views = TasmoModelFactory.bindsAsViews(viewBindings);
        t.initModel(views);
        
        ObjectId user1 = t.write(EventBuilder.create(t.idProvider(), "User", tenantId, actorId).set("userName", "ted").build());

        ObjectNode view = t.readView(tenantIdAndCentricId, actorId, new ObjectId(idCentricViewClassName, user1.getId()));
        System.out.println("Centric View:" + mapper.writeValueAsString(view));
        // assert id centric
        t.addExpectation(user1, idCentricViewClassName, idCentricViewFieldName, new ObjectId[]{user1}, "userName", "ted");
        //t.assertExpectation(tenantIdAndCentricId);
        t.clearExpectations();

        // assert non id centric
        tenantIdAndCentricId = new TenantIdAndCentricId(tenantId, Id.NULL);
        view = t.readView(tenantIdAndCentricId, actorId, new ObjectId(nonIdCentricViewClassName, user1.getId()));
        System.out.println("System View:" + mapper.writeValueAsString(view));

        t.addExpectation(user1, nonIdCentricViewClassName, nonIdCentricViewFieldName, new ObjectId[]{user1}, "userName", "ted");
        //t.assertExpectation(tenantIdAndCentricId);
        t.clearExpectations();

    }

}
