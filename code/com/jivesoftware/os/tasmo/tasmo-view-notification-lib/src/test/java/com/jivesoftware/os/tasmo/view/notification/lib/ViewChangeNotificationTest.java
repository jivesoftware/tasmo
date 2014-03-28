/*
 * Copyright 2014 pete.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.jivesoftware.os.tasmo.view.notification.lib;

import com.jivesoftware.os.tasmo.event.api.ReservedFields;
import com.jivesoftware.os.tasmo.event.api.write.EventBuilder;
import com.jivesoftware.os.tasmo.id.ObjectId;
import com.jivesoftware.os.tasmo.model.process.ModifiedViewInfo;
import java.util.Arrays;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 *
 * @author pete
 */
public class ViewChangeNotificationTest extends BaseViewNotificationTest {

    String eventModel =
        "User:userName(value),creationDate(value),manager(ref)|Content:location(ref)|Version:parent(ref),authors(refs),subject(value),body(value)|"
        + "Container:name(value),creationDate(value),owner(ref)";

    //TODO - longer path test and existence transition support
    @Test
    public void testRefValue() throws Exception {
        String viewModel = "VersionAuthorsView::pathId1::Version.authors.refs.User|User.username";
        initModel(eventModel, viewModel);

        ObjectId userId1 = write(EventBuilder.create(idProvider, "User", tenantId, actorId).set("username", "ted").build());
        ObjectId userId2 = write(EventBuilder.create(idProvider, "User", tenantId, actorId).set("username", "todd").build());

        ObjectId versionId = write(EventBuilder.create(idProvider, "Version", tenantId, actorId).set("authors", Arrays.asList(userId1, userId2)).build());

        ObjectId viewId = new ObjectId("VersionAuthorsView", versionId.getId());
        ModifiedViewInfo expected = new ModifiedViewInfo(tenantIdAndCentricId(), viewId);
        ModifiedViewInfo expectedGlobal = new ModifiedViewInfo(globalTenantIdAndCentricId(), viewId);

        Assert.assertTrue(getModifiedViews().contains(expected));
        Assert.assertTrue(getModifiedViews().contains(expectedGlobal));
        getModifiedViews().clear();

        write(EventBuilder.update(userId2, tenantId, actorId).set("username", "toddd").build());

        Assert.assertTrue(getModifiedViews().contains(expected));
        Assert.assertTrue(getModifiedViews().contains(expectedGlobal));
        getModifiedViews().clear();

        write(EventBuilder.update(userId2, tenantId, actorId).set(ReservedFields.DELETED, true).build());

        Assert.assertTrue(getModifiedViews().contains(expected));
        Assert.assertTrue(getModifiedViews().contains(expectedGlobal));
        getModifiedViews().clear();

        write(EventBuilder.update(userId2, tenantId, actorId).set(ReservedFields.DELETED, false).build());

        Assert.assertTrue(getModifiedViews().contains(expected));
        Assert.assertTrue(getModifiedViews().contains(expectedGlobal));
        getModifiedViews().clear();

        write(EventBuilder.update(versionId, tenantId, actorId).set("authors", Arrays.asList(userId1)).build());

        Assert.assertTrue(getModifiedViews().contains(expected));
        Assert.assertTrue(getModifiedViews().contains(expectedGlobal));
        getModifiedViews().clear();

        write(EventBuilder.update(versionId, tenantId, actorId).clear("authors").build());

        Assert.assertTrue(getModifiedViews().contains(expected));
        Assert.assertTrue(getModifiedViews().contains(expectedGlobal));
        getModifiedViews().clear();

        write(EventBuilder.update(versionId, tenantId, actorId).set(ReservedFields.DELETED, true).build());

        Assert.assertTrue(getModifiedViews().contains(expected));
        Assert.assertTrue(getModifiedViews().contains(expectedGlobal));
        getModifiedViews().clear();
    }

    @Test
    public void testBackRefValue() throws Exception {
        String viewModel = "UserVersionsView::pathId1::User.backRefs.Version.authors.|Version.body";
        initModel(eventModel, viewModel);

        ObjectId userId1 = write(EventBuilder.create(idProvider, "User", tenantId, actorId).build());
        ObjectId userId2 = write(EventBuilder.create(idProvider, "User", tenantId, actorId).build());

        ObjectId versionId = write(EventBuilder.create(idProvider, "Version", tenantId, actorId).set("body", "awesome").
            set("authors", Arrays.asList(userId1, userId2)).build());

        ObjectId user1ViewId = new ObjectId("UserVersionsView", userId1.getId());
        ModifiedViewInfo expectedUser1 = new ModifiedViewInfo(tenantIdAndCentricId(), user1ViewId);
        ModifiedViewInfo expectedGlobalUser1 = new ModifiedViewInfo(globalTenantIdAndCentricId(), user1ViewId);

        ObjectId user2ViewId = new ObjectId("UserVersionsView", userId2.getId());
        ModifiedViewInfo expectedUser2 = new ModifiedViewInfo(tenantIdAndCentricId(), user2ViewId);
        ModifiedViewInfo expectedGlobalUser2 = new ModifiedViewInfo(globalTenantIdAndCentricId(), user2ViewId);


        Assert.assertTrue(getModifiedViews().contains(expectedUser1));
        Assert.assertTrue(getModifiedViews().contains(expectedGlobalUser1));
        Assert.assertTrue(getModifiedViews().contains(expectedUser2));
        Assert.assertTrue(getModifiedViews().contains(expectedGlobalUser2));
        getModifiedViews().clear();

        write(EventBuilder.update(userId2, tenantId, actorId).set(ReservedFields.DELETED, true).build());

        Assert.assertTrue(getModifiedViews().contains(expectedUser2));
        Assert.assertTrue(getModifiedViews().contains(expectedGlobalUser2));
        getModifiedViews().clear();

        write(EventBuilder.update(userId2, tenantId, actorId).set(ReservedFields.DELETED, false).build());

        Assert.assertTrue(getModifiedViews().contains(expectedUser2));
        Assert.assertTrue(getModifiedViews().contains(expectedGlobalUser2));
        getModifiedViews().clear();

        write(EventBuilder.update(versionId, tenantId, actorId).set("authors", Arrays.asList(userId1)).build());

        Assert.assertTrue(getModifiedViews().contains(expectedUser1));
        Assert.assertTrue(getModifiedViews().contains(expectedGlobalUser1));
        Assert.assertTrue(getModifiedViews().contains(expectedUser2));
        Assert.assertTrue(getModifiedViews().contains(expectedGlobalUser2));
        getModifiedViews().clear();

        write(EventBuilder.update(versionId, tenantId, actorId).clear("authors").build());

        Assert.assertTrue(getModifiedViews().contains(expectedUser1));
        Assert.assertTrue(getModifiedViews().contains(expectedGlobalUser1));
        getModifiedViews().clear();

        write(EventBuilder.update(versionId, tenantId, actorId).set(ReservedFields.DELETED, true).build());

        Assert.assertFalse(getModifiedViews().contains(expectedUser1));
        Assert.assertFalse(getModifiedViews().contains(expectedGlobalUser1));
        Assert.assertFalse(getModifiedViews().contains(expectedUser2));
        Assert.assertFalse(getModifiedViews().contains(expectedGlobalUser2));
        getModifiedViews().clear();
    }

    @Test
    public void testRefsLatestBackrefValue() throws Exception {
        String viewModel = "VersionAuthorContainerOwner::path1::Version.authors.refs.User|User.latest_backRef.Container.owner|Container.name";
        initModel(eventModel, viewModel);

        ObjectId user1 = write(EventBuilder.create(idProvider, "User", tenantId, actorId).build());
        ObjectId user2 = write(EventBuilder.create(idProvider, "User", tenantId, actorId).build());

        ObjectId container1 = write(EventBuilder.create(idProvider, "Container", tenantId, actorId).set("name", "awesome").build());
        ObjectId container2 = write(EventBuilder.create(idProvider, "Container", tenantId, actorId).set("name", "lame").build());

        Assert.assertTrue(getModifiedViews().isEmpty());

        ObjectId version1 = write(EventBuilder.create(idProvider, "Version", tenantId, actorId).set("authors", Arrays.asList(user1, user2)).build());

        ModifiedViewInfo centricViewInfo = new ModifiedViewInfo(tenantIdAndCentricId, new ObjectId("VersionAuthorContainerOwner", version1.getId()));
        ModifiedViewInfo globalViewInfo = new ModifiedViewInfo(tenantIdAndCentricId, new ObjectId("VersionAuthorContainerOwner", version1.getId()));
        Assert.assertTrue(getModifiedViews().contains(centricViewInfo));
        Assert.assertTrue(getModifiedViews().contains(globalViewInfo));
        getModifiedViews().clear();

        write(EventBuilder.update(container1, tenantId, actorId).set("owner", user1).build());

        Assert.assertTrue(getModifiedViews().contains(centricViewInfo));
        Assert.assertTrue(getModifiedViews().contains(globalViewInfo));
        getModifiedViews().clear();

        write(EventBuilder.update(container2, tenantId, actorId).set("owner", user2).build());

        Assert.assertTrue(getModifiedViews().contains(centricViewInfo));
        Assert.assertTrue(getModifiedViews().contains(globalViewInfo));
        getModifiedViews().clear();

        write(EventBuilder.update(container1, tenantId, actorId).clear("owner").build());

        Assert.assertTrue(getModifiedViews().contains(centricViewInfo));
        Assert.assertTrue(getModifiedViews().contains(globalViewInfo));
        getModifiedViews().clear();

        write(EventBuilder.update(user1, tenantId, actorId).set(ReservedFields.DELETED, true).build());

        Assert.assertTrue(getModifiedViews().contains(centricViewInfo));
        Assert.assertTrue(getModifiedViews().contains(globalViewInfo));
        getModifiedViews().clear();

    }
}