package com.jivesoftware.os.tasmo.lib;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.jivesoftware.os.jive.utils.id.Id;
import com.jivesoftware.os.jive.utils.id.ObjectId;
import com.jivesoftware.os.tasmo.event.api.write.EventBuilder;
import com.jivesoftware.os.tasmo.model.ViewBinding;
import com.jivesoftware.os.tasmo.model.Views;
import java.util.Arrays;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 *
 */
public class CentricIdTest extends BaseTest {

    @Test (dataProvider = "tasmoMaterializer", invocationCount = 1, singleThreaded = true, enabled = false)
    public void testCentricRefsField(TasmoMaterializerHarness t) throws Exception {
        String idCentricViewClassName = "IdCentricView";
        String idCentricViewFieldName = "idCentricUserInfo";

        List<ViewBinding> viewBindings = TasmoModelFactory
            .parseModelPathStrings(idCentricViewClassName + "::" + idCentricViewFieldName + "::Contacts.refs_users.centric_refs.User|User.userName,age");

        Id userBob = new Id(1);
        Id userJane = new Id(2);
        Views views = TasmoModelFactory.bindsAsViews(viewBindings);
        t.initModel(views);

        ObjectId user1 = t.write(EventBuilder.create(t.idProvider(), "User", tenantId, actorId, userBob).set("userName", "ted").build());
        ObjectId user2 = t.write(EventBuilder.create(t.idProvider(), "User", tenantId, actorId, userJane).set("userName", "teddy").build());

        ObjectId contacts = t.write(EventBuilder.create(t.idProvider(), "Contacts", tenantId, actorId, userBob)
                .set("refs_users", Arrays.asList(user1)).build());

        t.write(EventBuilder.update(contacts, tenantId, actorId, userJane)
                .set("refs_users", Arrays.asList(user2)).build());


        ObjectNode bobsContacts = t.readView(tenantId, actorId, new ObjectId(idCentricViewClassName, contacts.getId()), userBob);
        System.out.println("Bobs View:" + mapper.writeValueAsString(bobsContacts));

        ObjectNode janesContacts = t.readView(tenantId, actorId, new ObjectId(idCentricViewClassName, contacts.getId()), userJane);
        System.out.println("Janes View:" + mapper.writeValueAsString(janesContacts));

        ObjectNode globalContacts = t.readView(tenantId, actorId, new ObjectId(idCentricViewClassName, contacts.getId()), Id.NULL);
        System.out.println("Global View:" + mapper.writeValueAsString(globalContacts));

        Assert.assertNotNull(bobsContacts);
        Assert.assertNotNull(janesContacts);
        Assert.assertNotNull(globalContacts);

        Assert.assertNotEquals(bobsContacts, janesContacts);
        //Assert.assertEquals(globalContacts, janesContacts);
    }

}
