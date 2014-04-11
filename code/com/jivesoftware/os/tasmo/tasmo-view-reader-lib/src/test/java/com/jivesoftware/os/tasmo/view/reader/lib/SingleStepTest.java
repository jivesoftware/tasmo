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
package com.jivesoftware.os.tasmo.view.reader.lib;

import com.jivesoftware.os.tasmo.event.api.ReservedFields;
import com.jivesoftware.os.tasmo.event.api.write.EventBuilder;
import com.jivesoftware.os.tasmo.id.BaseView;
import com.jivesoftware.os.tasmo.id.Id;
import com.jivesoftware.os.tasmo.id.ObjectId;
import com.jivesoftware.os.tasmo.id.Ref;
import com.jivesoftware.os.tasmo.view.reader.api.ViewDescriptor;
import com.jivesoftware.os.tasmo.view.reader.api.ViewResponse;
import javax.annotation.Nullable;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 *
 * @author pete
 */
public class SingleStepTest extends BaseTasmoViewTest {
    //VersionView::path1::Version.backRefs.Content.ref_versionedContent|Content.ref_originalAuthor.ref.User|User.firstName

    String eventModel =
        "User:userName(value),creationDate(value),manager(ref)|Content:location(ref)|Version:parent(ref),authors(refs),subject(value),body(value)";

    private static interface Version extends BaseView {

        @Nullable
        String subject();

        @Nullable
        String body();
    };

    private static interface User extends BaseView {

        Ref<User> manager();
    };

    @Test
    public void testSetAndModify() throws Exception {
        String viewModel = "VersionView::path1::Version.subject,body";
        initModel(eventModel, viewModel);

        ObjectId objectId = write(EventBuilder.create(idProvider, "Version", tenantId, actorId, actorId).set("subject", "awesome").build());
        ViewResponse response = viewReader.readView(new ViewDescriptor(tenantId, actorId, new ObjectId("VersionView", objectId.getId())));

        Assert.assertNotNull(response);
        Assert.assertEquals(response.getStatusCode(), ViewResponse.StatusCode.OK);

        Version version = getView(response, Version.class);

        Assert.assertNotNull(version);
        Assert.assertEquals("awesome", version.subject());

        write(EventBuilder.update(objectId, tenantId, actorId, actorId).set("body", "awesomer").build());
        response = viewReader.readView(new ViewDescriptor(tenantId, actorId, new ObjectId("VersionView", objectId.getId())));

        Assert.assertNotNull(response);
        Assert.assertEquals(response.getStatusCode(), ViewResponse.StatusCode.OK);

        version = getView(response, Version.class);

        Assert.assertNotNull(version);
        Assert.assertEquals("awesome", version.subject());
        Assert.assertEquals("awesomer", version.body());

        write(EventBuilder.update(objectId, tenantId, actorId, actorId).set("body", "not awesomer").set("subject", "not awesome").build());
        response = viewReader.readView(new ViewDescriptor(tenantId, actorId, new ObjectId("VersionView", objectId.getId())));

        Assert.assertNotNull(response);
        Assert.assertEquals(response.getStatusCode(), ViewResponse.StatusCode.OK);

        version = getView(response, Version.class);

        Assert.assertNotNull(version);
        Assert.assertEquals("not awesome", version.subject());
        Assert.assertEquals("not awesomer", version.body());
    }

    @Test
    public void testSetAndClear() throws Exception {
        String viewModel = "VersionView::path1::Version.subject,body";
        initModel(eventModel, viewModel);

        ObjectId objectId = write(EventBuilder.create(idProvider, "Version", tenantId, actorId, actorId).
            set("subject", "awesome").set("body", "awesomer").build());
        ViewResponse response = viewReader.readView(new ViewDescriptor(tenantId, actorId, new ObjectId("VersionView", objectId.getId())));

        Assert.assertNotNull(response);
        Assert.assertEquals(response.getStatusCode(), ViewResponse.StatusCode.OK);

        Version version = getView(response, Version.class);

        Assert.assertNotNull(version);
        Assert.assertEquals("awesome", version.subject());
        Assert.assertEquals("awesomer", version.body());

        write(EventBuilder.update(objectId, tenantId, actorId, actorId).clear("body").build());
        response = viewReader.readView(new ViewDescriptor(tenantId, actorId, new ObjectId("VersionView", objectId.getId())));

        Assert.assertNotNull(response);
        Assert.assertEquals(response.getStatusCode(), ViewResponse.StatusCode.OK);

        version = getView(response, Version.class);

        Assert.assertNotNull(version);
        Assert.assertEquals("awesome", version.subject());
        Assert.assertNull(version.body());

        write(EventBuilder.update(objectId, tenantId, actorId, actorId).clear("subject").build());
        response = viewReader.readView(new ViewDescriptor(tenantId, actorId, new ObjectId("VersionView", objectId.getId())));

        Assert.assertNotNull(response);
        Assert.assertEquals(response.getStatusCode(), ViewResponse.StatusCode.OK);

        version = getView(response, Version.class);

        Assert.assertNotNull(version);
        Assert.assertNull(version.subject());
        Assert.assertNull(version.body());
    }

    @Test
    public void testDeleteAndRecreate() throws Exception {
        String viewModel = "VersionView::path1::Version.subject,body";
        initModel(eventModel, viewModel);

        ObjectId objectId = write(EventBuilder.create(idProvider, "Version", tenantId, actorId, actorId).
            set("subject", "awesome").set("body", "awesomer").build());
        ViewResponse response = viewReader.readView(new ViewDescriptor(tenantId, actorId, new ObjectId("VersionView", objectId.getId())));

        Assert.assertNotNull(response);
        Assert.assertEquals(response.getStatusCode(), ViewResponse.StatusCode.OK);

        Version version = getView(response, Version.class);

        Assert.assertNotNull(version);
        Assert.assertEquals("awesome", version.subject());
        Assert.assertEquals("awesomer", version.body());

        write(EventBuilder.update(objectId, tenantId, actorId, actorId).set(ReservedFields.DELETED, true).build());
        response = viewReader.readView(new ViewDescriptor(tenantId, actorId, new ObjectId("VersionView", objectId.getId())));

        Assert.assertNotNull(response);
        Assert.assertEquals(response.getStatusCode(), ViewResponse.StatusCode.NOT_FOUND);

        write(EventBuilder.update(objectId, tenantId, actorId, actorId).set("subject", "awesome").set("body", "awesomer").build());
        response = viewReader.readView(new ViewDescriptor(tenantId, actorId, new ObjectId("VersionView", objectId.getId())));

        Assert.assertNotNull(response);
        Assert.assertEquals(response.getStatusCode(), ViewResponse.StatusCode.OK);

        version = getView(response, Version.class);

        Assert.assertNotNull(version);
        Assert.assertEquals("awesome", version.subject());
        Assert.assertEquals("awesomer", version.body());

    }

    @Test
    public void testNotVisible() throws Exception {
        String viewModel = "VersionView::path1::Version.subject,body";
        initModel(eventModel, viewModel);

        ObjectId objectId = write(EventBuilder.create(idProvider, "Version", tenantId, actorId, actorId).
            set("subject", "awesome").set("body", "awesomer").build());
        ViewResponse response = viewReader.readView(new ViewDescriptor(tenantId, actorId, new ObjectId("VersionView", objectId.getId())));

        Assert.assertNotNull(response);
        Assert.assertEquals(response.getStatusCode(), ViewResponse.StatusCode.OK);

        Version version = getView(response, Version.class);

        Assert.assertNotNull(version);
        Assert.assertEquals("awesome", version.subject());
        Assert.assertEquals("awesomer", version.body());

        permittedIds.add(Id.NULL);

        response = viewReader.readView(new ViewDescriptor(tenantId, actorId, new ObjectId("VersionView", objectId.getId())));

        Assert.assertNotNull(response);
        Assert.assertEquals(response.getStatusCode(), ViewResponse.StatusCode.FORBIDDEN);
    }

    @Test
    public void testReadRefAsValue() throws Exception {

        String viewModel = "UserView::path1::User.manager";
        initModel(eventModel, viewModel);

        ObjectId manager = write(EventBuilder.create(idProvider, "User", tenantId, actorId, actorId).build());
        ObjectId employee = write(EventBuilder.create(idProvider, "User", tenantId, actorId, actorId).set("manager", manager).build());
        ViewResponse response = viewReader.readView(new ViewDescriptor(tenantId, actorId, new ObjectId("UserView", employee.getId())));

        Assert.assertNotNull(response);
        Assert.assertEquals(response.getStatusCode(), ViewResponse.StatusCode.OK);

        User userView = getView(response, User.class);

        Assert.assertNotNull(userView);
        Ref<User> managerRef = userView.manager();
        Assert.assertEquals(manager, managerRef.getObjectId());
    }
}
