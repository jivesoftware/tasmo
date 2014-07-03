

package com.jivesoftware.os.tasmo.lib;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.jivesoftware.os.jive.utils.id.ObjectId;
import com.jivesoftware.os.tasmo.event.api.ReservedFields;
import com.jivesoftware.os.tasmo.event.api.write.EventBuilder;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 *
 * @author jonathan.colt
 */
public class WTF extends WTFBase {

    @Test
    public void testDeleteRootWithInitialBackRefOnly() throws Exception {
        String viewClass = "ViewToDelete";
        String viewClass2 = "AnotherViewToDelete";
        initModelPaths(
                viewClass + "::path4::Document.latest_backRef.Tag.ref_tagged|Tag.name",
                viewClass2 + "::path5::Document.latest_backRef.Tag.ref_tagged|Tag.name");

        ObjectId docId = write(EventBuilder.create(idProvider, "Document", tenantId, actorId).build());
        ObjectId tagId = write(EventBuilder.create(idProvider, "Tag", tenantId, actorId)
                .set("ref_tagged", docId)
                .set("name", "foo")
                .build());


        ObjectNode view = readView(tenantIdAndCentricId, actorId, new ObjectId(viewClass, docId.getId()));
        Assert.assertNotNull(view);

        ObjectNode view2 = readView(tenantIdAndCentricId, actorId, new ObjectId(viewClass2, docId.getId()));
        Assert.assertNotNull(view2);

        write(EventBuilder.update(docId, tenantId, actorId).set(ReservedFields.DELETED, true).build());


        view = readView(tenantIdAndCentricId, actorId, new ObjectId(viewClass, docId.getId()));
        Assert.assertNull(view);

        view2 = readView(tenantIdAndCentricId, actorId, new ObjectId(viewClass2, docId.getId()));
        Assert.assertNull(view2);

        write(EventBuilder.update(tagId, tenantId, actorId).set("ref_tagged", docId).build());
        view = readView(tenantIdAndCentricId, actorId, new ObjectId(viewClass, docId.getId()));
        System.out.println("view1 = "+mapper.writeValueAsString(view));

        view2 = readView(tenantIdAndCentricId, actorId, new ObjectId(viewClass2, docId.getId()));
        System.out.println("view2 = "+mapper.writeValueAsString(view2));

        Assert.assertNull(view);
        Assert.assertNull(view2);

    }
}
