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
import com.jivesoftware.os.tasmo.id.BaseEvent;
import com.jivesoftware.os.tasmo.id.BaseView;
import com.jivesoftware.os.tasmo.id.ObjectId;
import com.jivesoftware.os.tasmo.model.process.ModifiedViewInfo;
import com.jivesoftware.os.tasmo.view.reader.api.BackRef;
import com.jivesoftware.os.tasmo.view.reader.api.BackRefType;
import java.util.Arrays;
import javax.annotation.Nullable;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 *
 * @author pete
 */
public class ViewChangeNotificationTest extends BaseViewNotificationTest {

    private static interface ContentView extends BaseView {

        @Nullable
        Container location();

        interface Container {

            @Nullable
            String name();

            @Nullable
            Long creationDate();
        }
    }

    private static interface VersionEvent extends BaseEvent {
    }

    private static interface VersionAuthorsView extends BaseView {

        @Nullable
        User[] authors();

        interface User {

            @Nullable
            String userName();
        }
    }

    private static interface UserVersionsView extends BaseView {

        @Nullable
        @BackRef(type = BackRefType.ALL, via = "authors", from = VersionEvent.class)
        Version[] authors();

        interface Version {

            @Nullable
            String body();
        }
    }

    private static interface UserVersionsLatestView extends BaseView {

        @Nullable
        @BackRef(type = BackRefType.LATEST, via = "authors", from = VersionEvent.class)
        Version authors();

        interface Version {

            @Nullable
            String body();
        }
    }

    private static interface UserVersionsCountView extends BaseView {

        @Nullable
        @BackRef(type = BackRefType.COUNT, via = "authors", from = VersionEvent.class)
        int authors();
    }
    String eventModel =
        "User:userName(value),creationDate(value),manager(ref)|Content:location(ref)|Version:parent(ref),authors(refs),subject(value),body(value)|"
        + "Container:name(value),creationDate(value),owner(ref)";

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
        Assert.assertTrue(getModifiedViews().contains(expectedUser2));
        Assert.assertTrue(getModifiedViews().contains(expectedGlobalUser2));
        getModifiedViews().clear();

        write(EventBuilder.update(versionId, tenantId, actorId).set(ReservedFields.DELETED, true).build());

        Assert.assertFalse(getModifiedViews().contains(expectedUser1));
        Assert.assertFalse(getModifiedViews().contains(expectedGlobalUser1));
        Assert.assertFalse(getModifiedViews().contains(expectedUser2));
        Assert.assertFalse(getModifiedViews().contains(expectedGlobalUser2));
        getModifiedViews().clear();
    }
}