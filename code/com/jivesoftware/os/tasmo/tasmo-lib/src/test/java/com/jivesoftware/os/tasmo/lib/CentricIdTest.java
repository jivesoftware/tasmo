package com.jivesoftware.os.tasmo.lib;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.jivesoftware.os.jive.utils.id.Id;
import com.jivesoftware.os.jive.utils.id.ObjectId;
import com.jivesoftware.os.jive.utils.id.TenantIdAndCentricId;
import com.jivesoftware.os.tasmo.event.api.write.EventBuilder;
import com.jivesoftware.os.tasmo.model.ViewBinding;
import com.jivesoftware.os.tasmo.model.Views;
import java.util.List;
import org.testng.Assert;
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

        Id userId = actorId;
        Views views = TasmoModelFactory.bindsAsViews(viewBindings);
        t.initModel(views);

        ObjectId user1 = t.write(EventBuilder.create(t.idProvider(), "User", tenantId, actorId, userId).set("userName", "ted").build());

        // assert id centric
        t.addExpectation(user1, idCentricViewClassName, idCentricViewFieldName, new ObjectId[]{ user1 }, "userName", "ted");
        ObjectNode view = t.readView(tenantId, actorId, new ObjectId(idCentricViewClassName, user1.getId()), userId);
        System.out.println("Centric View:" + mapper.writeValueAsString(view));
        t.assertExpectation(new TenantIdAndCentricId(tenantId, userId));
        t.clearExpectations();

        // assert non id centric
        t.addExpectation(user1, nonIdCentricViewClassName, nonIdCentricViewFieldName, new ObjectId[]{ user1 }, "userName", "ted");
        view = t.readView(tenantId, actorId, new ObjectId(nonIdCentricViewClassName, user1.getId()), Id.NULL);
        System.out.println("System View:" + mapper.writeValueAsString(view));
        t.assertExpectation(new TenantIdAndCentricId(tenantId, Id.NULL));
        t.clearExpectations();

    }

    @Test (dataProvider = "tasmoMaterializer", invocationCount = 1, singleThreaded = true)
    public void testCentricOnlyViews(TasmoMaterializerHarness t) throws Exception {
        String idCentricViewClassName = "IdCentricView";
        String idCentricViewFieldName = "idCentricUserInfo";

        List<ViewBinding> viewBindings = TasmoModelFactory
            .parseModelPathStrings(true, idCentricViewClassName + "::" + idCentricViewFieldName + "::User.userName,age");

        Id userId = actorId;
        Views views = TasmoModelFactory.bindsAsViews(viewBindings);
        t.initModel(views);

        ObjectId user1 = t.write(EventBuilder.create(t.idProvider(), "User", tenantId, actorId, userId).set("userName", "ted").build());

        // assert id centric
        t.addExpectation(user1, idCentricViewClassName, idCentricViewFieldName, new ObjectId[]{ user1 }, "userName", "ted");
        ObjectNode view = t.readView(tenantId, actorId, new ObjectId(idCentricViewClassName, user1.getId()), userId);
        System.out.println("Centric View:" + mapper.writeValueAsString(view));
        t.assertExpectation(new TenantIdAndCentricId(tenantId, userId));
        t.clearExpectations();

        // assert non id centric
        view = t.readView(tenantId, actorId, new ObjectId(idCentricViewClassName, user1.getId()), Id.NULL);
        System.out.println("System View:" + mapper.writeValueAsString(view));
        Assert.assertNull(view);
    }

    @Test (dataProvider = "tasmoMaterializer", invocationCount = 1, singleThreaded = true)
    public void testCentricOnlyViewsWithNonCentricEvent(TasmoMaterializerHarness t) throws Exception {
        String idCentricViewClassName = "IdCentricView";
        String idCentricViewFieldName = "idCentricUserInfo";

        List<ViewBinding> viewBindings = TasmoModelFactory
            .parseModelPathStrings(true, idCentricViewClassName + "::" + idCentricViewFieldName + "::User.userName,age");

        Id userId = actorId;
        Views views = TasmoModelFactory.bindsAsViews(viewBindings);
        t.initModel(views);

        ObjectId user1 = t.write(EventBuilder.create(t.idProvider(), "User", tenantId, actorId, Id.NULL).set("userName", "ted").build());

        // assert id centric
        ObjectNode view = t.readView(tenantId, actorId, new ObjectId(idCentricViewClassName, user1.getId()), userId);
        System.out.println("Centric View:" + mapper.writeValueAsString(view));
        Assert.assertNull(view);
    }

}
