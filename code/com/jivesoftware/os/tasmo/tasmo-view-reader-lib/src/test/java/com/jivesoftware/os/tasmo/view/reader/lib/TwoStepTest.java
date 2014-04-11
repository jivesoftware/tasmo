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
import com.jivesoftware.os.tasmo.id.BaseEvent;
import com.jivesoftware.os.tasmo.id.BaseView;
import com.jivesoftware.os.tasmo.id.ObjectId;
import com.jivesoftware.os.tasmo.view.reader.api.ViewDescriptor;
import com.jivesoftware.os.tasmo.view.reader.api.ViewId;
import com.jivesoftware.os.tasmo.view.reader.api.ViewResponse;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import javax.annotation.Nullable;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 *
 * @author pete
 */
public class TwoStepTest extends BaseTasmoViewTest {
    //VersionView::path1::Version.backRefs.Content.ref_versionedContent|Content.ref_originalAuthor.ref.User|User.firstName

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
        Version[] all_authors();

        interface Version {

            @Nullable
            String body();
        }
    }

    private static interface UserVersionsLatestView extends BaseView {

        @Nullable
        Version latest_authors();

        interface Version {

            @Nullable
            String body();
        }
    }

    private static interface UserVersionsCountView extends BaseView {

        @Nullable
        int count_authors();
    }
    String eventModel =
        "User:userName(value),creationDate(value),manager(ref)|Content:location(ref)|Version:parent(ref),authors(refs),subject(value),body(value)|"
        + "Container:name(value),creationDate(value),owner(ref)";

    @Test
    public void testRefToValue() throws Exception {
        //add
        String viewModel = "ContentView::path1::Content.location.ref.Container|Container.name,creationDate";
        initModel(eventModel, viewModel);

        ObjectId containerId = write(EventBuilder.create(idProvider, "Container", tenantId, actorId).
            set("name", "awesome").build());

        ObjectId contentId = write(EventBuilder.create(idProvider, "Content", tenantId, actorId).
            set("location", containerId).build());

        ViewId viewId = ViewId.ofId(contentId.getId(), ContentView.class);
        ViewResponse response = viewReader.readView(new ViewDescriptor(tenantIdAndCentricId, actorId, viewId));

        Assert.assertNotNull(response);
        Assert.assertEquals(response.getStatusCode(), ViewResponse.StatusCode.OK);

        ContentView content = getView(response, ContentView.class);
        Assert.assertNotNull(content);

        ContentView.Container container = content.location();
        Assert.assertNotNull(container);

        Assert.assertNull(container.creationDate());
        Assert.assertEquals(container.name(), "awesome");

        //update value
        write(EventBuilder.update(containerId, tenantId, actorId).set("name", "not awesome").set("creationDate", 12345l).build());

        response = viewReader.readView(new ViewDescriptor(tenantIdAndCentricId, actorId, viewId));

        Assert.assertNotNull(response);
        Assert.assertEquals(response.getStatusCode(), ViewResponse.StatusCode.OK);

        content = getView(response, ContentView.class);
        Assert.assertNotNull(content);

        container = content.location();
        Assert.assertNotNull(container);

        Assert.assertEquals(container.name(), "not awesome");
        Assert.assertEquals(container.creationDate(), (Long) 12345l);

        write(EventBuilder.update(containerId, tenantId, actorId).clear("name").build());

        response = viewReader.readView(new ViewDescriptor(tenantIdAndCentricId, actorId, viewId));

        Assert.assertNotNull(response);
        Assert.assertEquals(response.getStatusCode(), ViewResponse.StatusCode.OK);

        content = getView(response, ContentView.class);
        Assert.assertNotNull(content);

        container = content.location();
        Assert.assertNotNull(container);

        Assert.assertNull(container.name());
        Assert.assertEquals(container.creationDate(), (Long) 12345l);

        //clear value
        write(EventBuilder.update(containerId, tenantId, actorId).clear("name").build());

        response = viewReader.readView(new ViewDescriptor(tenantIdAndCentricId, actorId, viewId));

        Assert.assertNotNull(response);
        Assert.assertEquals(response.getStatusCode(), ViewResponse.StatusCode.OK);

        content = getView(response, ContentView.class);
        Assert.assertNotNull(content);

        container = content.location();
        Assert.assertNotNull(container);

        Assert.assertNull(container.name());
        Assert.assertEquals(container.creationDate(), (Long) 12345l);

        //dereference
        write(EventBuilder.update(contentId, tenantId, actorId).clear("location").build());

        response = viewReader.readView(new ViewDescriptor(tenantIdAndCentricId, actorId, viewId));

        Assert.assertNotNull(response);
        Assert.assertEquals(response.getStatusCode(), ViewResponse.StatusCode.OK);

        content = getView(response, ContentView.class);
        Assert.assertNotNull(content);

        container = content.location();
        Assert.assertNull(container);

        //re-reference
        write(EventBuilder.update(contentId, tenantId, actorId).set("location", containerId).build());

        response = viewReader.readView(new ViewDescriptor(tenantIdAndCentricId, actorId, viewId));

        Assert.assertNotNull(response);
        Assert.assertEquals(response.getStatusCode(), ViewResponse.StatusCode.OK);

        content = getView(response, ContentView.class);
        Assert.assertNotNull(content);

        container = content.location();
        Assert.assertNotNull(container);

        Assert.assertNull(container.name());
        Assert.assertEquals(container.creationDate(), (Long) 12345l);

        //delete value
        write(EventBuilder.update(containerId, tenantId, actorId).set(ReservedFields.DELETED, true).build());
        response = viewReader.readView(new ViewDescriptor(tenantIdAndCentricId, actorId, viewId));

        Assert.assertNotNull(response);
        Assert.assertEquals(response.getStatusCode(), ViewResponse.StatusCode.OK);

        content = getView(response, ContentView.class);
        Assert.assertNotNull(content);

        container = content.location();
        Assert.assertNull(container);


        //undelete
        write(EventBuilder.update(containerId, tenantId, actorId).set(ReservedFields.DELETED, false).build());

        response = viewReader.readView(new ViewDescriptor(tenantIdAndCentricId, actorId, viewId));

        Assert.assertNotNull(response);
        Assert.assertEquals(response.getStatusCode(), ViewResponse.StatusCode.OK);

        content = getView(response, ContentView.class);
        Assert.assertNotNull(content);

        container = content.location();
        Assert.assertNotNull(container);

        Assert.assertNull(container.name());
        Assert.assertNull(container.creationDate());

        //invisible value
        permittedIds.add(contentId.getId()); //this will make container invisible
        response = viewReader.readView(new ViewDescriptor(tenantIdAndCentricId, actorId, viewId));

        Assert.assertNotNull(response);
        Assert.assertEquals(response.getStatusCode(), ViewResponse.StatusCode.OK);

        content = getView(response, ContentView.class);
        Assert.assertNotNull(content);

        container = content.location();
        Assert.assertNull(container);

        //now all is visible
        permittedIds.clear();

        //invisible root
        permittedIds.add(containerId.getId());
        response = viewReader.readView(new ViewDescriptor(tenantIdAndCentricId, actorId, viewId));

        Assert.assertNotNull(response);
        Assert.assertEquals(response.getStatusCode(), ViewResponse.StatusCode.FORBIDDEN);

        permittedIds.clear();

        //delete root
        write(EventBuilder.update(contentId, tenantId, actorId).set(ReservedFields.DELETED, true).build());

        response = viewReader.readView(new ViewDescriptor(tenantIdAndCentricId, actorId, viewId));

        Assert.assertNotNull(response);
        Assert.assertEquals(response.getStatusCode(), ViewResponse.StatusCode.NOT_FOUND);
    }

    @Test
    public void testMultiRefToValue() throws Exception {
        //add
        String viewModel = "VersionAuthorsView::path1::Version.authors.refs.User|User.userName";
        initModel(eventModel, viewModel);

        ObjectId userId1 = write(EventBuilder.create(idProvider, "User", tenantId, actorId).
            set("userName", "ted").build());

        ObjectId userId2 = write(EventBuilder.create(idProvider, "User", tenantId, actorId).
            set("userName", "tod").build());

        ObjectId versionId = write(EventBuilder.create(idProvider, "Version", tenantId, actorId).
            set("authors", Arrays.asList(userId1)).build());

        ViewId viewId = ViewId.ofId(versionId.getId(), VersionAuthorsView.class);
        ViewResponse response = viewReader.readView(new ViewDescriptor(tenantIdAndCentricId, actorId, viewId));

        Assert.assertNotNull(response);
        Assert.assertEquals(response.getStatusCode(), ViewResponse.StatusCode.OK);

        VersionAuthorsView version = getView(response, VersionAuthorsView.class);
        Assert.assertNotNull(version);

        VersionAuthorsView.User[] authors = version.authors();
        Assert.assertNotNull(authors);
        Assert.assertEquals(authors.length, 1);
        Assert.assertEquals(authors[0].userName(), "ted");

        write(EventBuilder.update(versionId, tenantId, actorId).
            set("authors", Arrays.asList(userId2)).build());

        response = viewReader.readView(new ViewDescriptor(tenantIdAndCentricId, actorId, viewId));

        Assert.assertNotNull(response);
        Assert.assertEquals(response.getStatusCode(), ViewResponse.StatusCode.OK);

        version = getView(response, VersionAuthorsView.class);
        Assert.assertNotNull(version);

        authors = version.authors();
        Assert.assertNotNull(authors);
        Assert.assertEquals(authors.length, 1);
        Assert.assertEquals(authors[0].userName(), "tod");

        write(EventBuilder.update(versionId, tenantId, actorId).
            set("authors", Arrays.asList(userId1, userId2)).build());

        response = viewReader.readView(new ViewDescriptor(tenantIdAndCentricId, actorId, viewId));

        Assert.assertNotNull(response);
        Assert.assertEquals(response.getStatusCode(), ViewResponse.StatusCode.OK);

        version = getView(response, VersionAuthorsView.class);
        Assert.assertNotNull(version);

        authors = version.authors();
        Assert.assertNotNull(authors);

        Assert.assertEquals(authors.length, 2);

        Set<String> names = new HashSet<>();
        for (VersionAuthorsView.User author : authors) {
            names.add(author.userName());
        }

        Assert.assertTrue(names.contains("ted"));
        Assert.assertTrue(names.contains("tod"));

        //clear refs
        write(EventBuilder.update(versionId, tenantId, actorId).
            clear("authors").build());

        response = viewReader.readView(new ViewDescriptor(tenantIdAndCentricId, actorId, viewId));

        Assert.assertNotNull(response);
        Assert.assertEquals(response.getStatusCode(), ViewResponse.StatusCode.OK);

        version = getView(response, VersionAuthorsView.class);
        Assert.assertNotNull(version);

        authors = version.authors();
        Assert.assertEquals(authors.length, 0);

        //reset
        write(EventBuilder.update(versionId, tenantId, actorId).
            set("authors", Arrays.asList(userId1, userId2)).build());

        response = viewReader.readView(new ViewDescriptor(tenantIdAndCentricId, actorId, viewId));

        Assert.assertNotNull(response);
        Assert.assertEquals(response.getStatusCode(), ViewResponse.StatusCode.OK);

        version = getView(response, VersionAuthorsView.class);
        Assert.assertNotNull(version);

        authors = version.authors();
        Assert.assertNotNull(authors);

        Assert.assertEquals(authors.length, 2);

        names = new HashSet<>();
        for (VersionAuthorsView.User author : authors) {
            names.add(author.userName());
        }

        Assert.assertTrue(names.contains("ted"));
        Assert.assertTrue(names.contains("tod"));

        //one invisible
        permittedIds.add(versionId.getId());
        permittedIds.add(userId1.getId());

        response = viewReader.readView(new ViewDescriptor(tenantIdAndCentricId, actorId, viewId));

        Assert.assertNotNull(response);
        Assert.assertEquals(response.getStatusCode(), ViewResponse.StatusCode.OK);

        version = getView(response, VersionAuthorsView.class);
        Assert.assertNotNull(version);

        authors = version.authors();
        Assert.assertNotNull(authors);

        Assert.assertEquals(authors.length, 1);

        VersionAuthorsView.User user1 = authors[0];
        Assert.assertNotNull(user1);

        Assert.assertEquals(user1.userName(), "ted");

        //none visible
        permittedIds.remove(userId1.getId());

        response = viewReader.readView(new ViewDescriptor(tenantIdAndCentricId, actorId, viewId));

        Assert.assertNotNull(response);
        Assert.assertEquals(response.getStatusCode(), ViewResponse.StatusCode.OK);

        version = getView(response, VersionAuthorsView.class);
        Assert.assertNotNull(version);

        authors = version.authors();
        Assert.assertEquals(authors.length, 0);

        permittedIds.clear(); //all visible again

        //one deleted
        write(EventBuilder.update(userId1, tenantId, actorId).
            set("deleted", true).build());

        response = viewReader.readView(new ViewDescriptor(tenantIdAndCentricId, actorId, viewId));

        Assert.assertNotNull(response);
        Assert.assertEquals(response.getStatusCode(), ViewResponse.StatusCode.OK);

        version = getView(response, VersionAuthorsView.class);
        Assert.assertNotNull(version);

        authors = version.authors();
        Assert.assertNotNull(authors);

        Assert.assertEquals(authors.length, 1);
        VersionAuthorsView.User user2 = authors[0];
        Assert.assertNotNull(user2);

        Assert.assertEquals(user2.userName(), "tod");

        //recreate
        write(EventBuilder.update(userId1, tenantId, actorId).
            set("deleted", false).build());

        response = viewReader.readView(new ViewDescriptor(tenantIdAndCentricId, actorId, viewId));

        Assert.assertNotNull(response);
        Assert.assertEquals(response.getStatusCode(), ViewResponse.StatusCode.OK);

        version = getView(response, VersionAuthorsView.class);
        Assert.assertNotNull(version);

        authors = version.authors();
        Assert.assertNotNull(authors);

        Assert.assertEquals(authors.length, 2);
        names = new HashSet<>();
        for (VersionAuthorsView.User author : authors) {
            String userName = author.userName();
            if (userName != null) {
                names.add(author.userName());
            }
        }

        Assert.assertFalse(names.contains("ted"));
        Assert.assertTrue(names.contains("tod"));

        //delete both
        write(EventBuilder.update(userId1, tenantId, actorId).
            set("deleted", true).build());
        write(EventBuilder.update(userId2, tenantId, actorId).
            set("deleted", true).build());

        response = viewReader.readView(new ViewDescriptor(tenantIdAndCentricId, actorId, viewId));

        Assert.assertNotNull(response);
        Assert.assertEquals(response.getStatusCode(), ViewResponse.StatusCode.OK);

        version = getView(response, VersionAuthorsView.class);
        Assert.assertNotNull(version);

        authors = version.authors();
        Assert.assertNotNull(authors);

        Assert.assertEquals(authors.length, 0);
    }

    @Test
    public void testBackRefsToValue() throws Exception {
        //add
        String viewModel = "UserVersionsView::path1::User.backRefs.Version.authors|Version.body";
        initModel(eventModel, viewModel);

        ObjectId userId1 = write(EventBuilder.create(idProvider, "User", tenantId, actorId).build());

        ObjectId versionId1 = write(EventBuilder.create(idProvider, "Version", tenantId, actorId).
            set("authors", Arrays.asList(userId1)).set("body", "body1").build());

        ViewId viewId = ViewId.ofId(userId1.getId(), UserVersionsView.class);
        ViewResponse response = viewReader.readView(new ViewDescriptor(tenantIdAndCentricId, actorId, viewId));

        Assert.assertNotNull(response);
        Assert.assertEquals(response.getStatusCode(), ViewResponse.StatusCode.OK);

        UserVersionsView userVersion = getView(response, UserVersionsView.class);
        Assert.assertNotNull(userVersion);

        UserVersionsView.Version[] versions = userVersion.all_authors();
        Assert.assertNotNull(versions);
        Assert.assertEquals(versions.length, 1);
        Assert.assertEquals(versions[0].body(), "body1");

        ObjectId versionId2 = write(EventBuilder.create(idProvider, "Version", tenantId, actorId).
            set("authors", Arrays.asList(userId1)).set("body", "body2").build());

        response = viewReader.readView(new ViewDescriptor(tenantIdAndCentricId, actorId, viewId));

        Assert.assertNotNull(response);
        Assert.assertEquals(response.getStatusCode(), ViewResponse.StatusCode.OK);

        userVersion = getView(response, UserVersionsView.class);
        Assert.assertNotNull(userVersion);

        versions = userVersion.all_authors();
        Assert.assertNotNull(versions);

        Assert.assertEquals(versions.length, 2);

        Set<String> bodies = new HashSet<>();
        for (UserVersionsView.Version version : versions) {
            bodies.add(version.body());
        }

        Assert.assertTrue(bodies.contains("body1"));
        Assert.assertTrue(bodies.contains("body2"));

        //dereference one
        write(EventBuilder.update(versionId1, tenantId, actorId).
            clear("authors").build());

        response = viewReader.readView(new ViewDescriptor(tenantIdAndCentricId, actorId, viewId));

        Assert.assertNotNull(response);
        Assert.assertEquals(response.getStatusCode(), ViewResponse.StatusCode.OK);

        userVersion = getView(response, UserVersionsView.class);
        Assert.assertNotNull(userVersion);

        versions = userVersion.all_authors();
        Assert.assertNotNull(versions);

        Assert.assertEquals(versions.length, 1);

        Assert.assertEquals(versions[0].body(), "body2");

        //rereference
        write(EventBuilder.update(versionId1, tenantId, actorId).
            set("authors", Arrays.asList(userId1)).build());

        response = viewReader.readView(new ViewDescriptor(tenantIdAndCentricId, actorId, viewId));

        Assert.assertNotNull(response);
        Assert.assertEquals(response.getStatusCode(), ViewResponse.StatusCode.OK);

        userVersion = getView(response, UserVersionsView.class);
        Assert.assertNotNull(userVersion);

        versions = userVersion.all_authors();
        Assert.assertNotNull(versions);

        Assert.assertEquals(versions.length, 2);

        bodies = new HashSet<>();
        for (UserVersionsView.Version version : versions) {
            bodies.add(version.body());
        }

        Assert.assertTrue(bodies.contains("body1"));
        Assert.assertTrue(bodies.contains("body2"));

        //delete one
        write(EventBuilder.update(versionId2, tenantId, actorId).
            set(ReservedFields.DELETED, true).build());

        response = viewReader.readView(new ViewDescriptor(tenantIdAndCentricId, actorId, viewId));

        Assert.assertNotNull(response);
        Assert.assertEquals(response.getStatusCode(), ViewResponse.StatusCode.OK);

        userVersion = getView(response, UserVersionsView.class);
        Assert.assertNotNull(userVersion);

        versions = userVersion.all_authors();
        Assert.assertNotNull(versions);

        Assert.assertEquals(versions.length, 1);

        Assert.assertEquals(versions[0].body(), "body1");

        //undelete
        write(EventBuilder.update(versionId2, tenantId, actorId).
            set(ReservedFields.DELETED, false).set("authors", Arrays.asList(userId1)).set("body", "body2").build());

        response = viewReader.readView(new ViewDescriptor(tenantIdAndCentricId, actorId, viewId));

        Assert.assertNotNull(response);
        Assert.assertEquals(response.getStatusCode(), ViewResponse.StatusCode.OK);

        userVersion = getView(response, UserVersionsView.class);
        Assert.assertNotNull(userVersion);

        versions = userVersion.all_authors();

        Assert.assertNotNull(versions);
        Assert.assertEquals(versions.length, 2);

        bodies = new HashSet<>();
        for (UserVersionsView.Version version : versions) {
            bodies.add(version.body());
        }

        Assert.assertTrue(bodies.contains("body1"));
        Assert.assertTrue(bodies.contains("body2"));

        //delete both
        write(EventBuilder.update(versionId1, tenantId, actorId).
            set(ReservedFields.DELETED, true).build());
        write(EventBuilder.update(versionId2, tenantId, actorId).
            set(ReservedFields.DELETED, true).build());

        response = viewReader.readView(new ViewDescriptor(tenantIdAndCentricId, actorId, viewId));

        Assert.assertNotNull(response);
        Assert.assertEquals(response.getStatusCode(), ViewResponse.StatusCode.OK);

        userVersion = getView(response, UserVersionsView.class);
        Assert.assertNotNull(userVersion);

        versions = userVersion.all_authors();
        Assert.assertNotNull(versions);

        Assert.assertEquals(versions.length, 0);
    }

    @Test
    public void testLatestBackRefToValue() throws Exception {
        //add
        String viewModel = "UserVersionsLatestView::path1::User.latest_backRef.Version.authors|Version.body";
        initModel(eventModel, viewModel);

        ObjectId userId1 = write(EventBuilder.create(idProvider, "User", tenantId, actorId).build());

        ObjectId versionId1 = write(EventBuilder.create(idProvider, "Version", tenantId, actorId).
            set("authors", Arrays.asList(userId1)).set("body", "body1").build());

        ViewId viewId = ViewId.ofId(userId1.getId(), UserVersionsLatestView.class);
        ViewResponse response = viewReader.readView(new ViewDescriptor(tenantIdAndCentricId, actorId, viewId));

        Assert.assertNotNull(response);
        Assert.assertEquals(response.getStatusCode(), ViewResponse.StatusCode.OK);

        UserVersionsLatestView userVersion = getView(response, UserVersionsLatestView.class);
        Assert.assertNotNull(userVersion);

        UserVersionsLatestView.Version version = userVersion.latest_authors();
        Assert.assertNotNull(version);
        Assert.assertEquals(version.body(), "body1");

        ObjectId versionId2 = write(EventBuilder.create(idProvider, "Version", tenantId, actorId).
            set("authors", Arrays.asList(userId1)).set("body", "body2").build());

        response = viewReader.readView(new ViewDescriptor(tenantIdAndCentricId, actorId, viewId));

        response = viewReader.readView(new ViewDescriptor(tenantIdAndCentricId, actorId, viewId));

        Assert.assertNotNull(response);
        Assert.assertEquals(response.getStatusCode(), ViewResponse.StatusCode.OK);

        userVersion = getView(response, UserVersionsLatestView.class);
        Assert.assertNotNull(userVersion);

        version = userVersion.latest_authors();
        Assert.assertNotNull(version);
        Assert.assertEquals(version.body(), "body2");

        //clear latest and expose next latest
        write(EventBuilder.update(versionId2, tenantId, actorId).
            clear("authors").build());

        response = viewReader.readView(new ViewDescriptor(tenantIdAndCentricId, actorId, viewId));

        Assert.assertNotNull(response);
        Assert.assertEquals(response.getStatusCode(), ViewResponse.StatusCode.OK);

        userVersion = getView(response, UserVersionsLatestView.class);
        Assert.assertNotNull(userVersion);

        version = userVersion.latest_authors();
        Assert.assertNotNull(version);
        Assert.assertEquals(version.body(), "body1");

        //re-reference
        write(EventBuilder.update(versionId2, tenantId, actorId).
            set("authors", Arrays.asList(userId1)).build());

        response = viewReader.readView(new ViewDescriptor(tenantIdAndCentricId, actorId, viewId));

        Assert.assertNotNull(response);
        Assert.assertEquals(response.getStatusCode(), ViewResponse.StatusCode.OK);

        userVersion = getView(response, UserVersionsLatestView.class);
        Assert.assertNotNull(userVersion);

        version = userVersion.latest_authors();
        Assert.assertNotNull(version);
        Assert.assertEquals(version.body(), "body2");

        //delete latest and expose next latest
        write(EventBuilder.update(versionId2, tenantId, actorId).
            set(ReservedFields.DELETED, true).build());

        response = viewReader.readView(new ViewDescriptor(tenantIdAndCentricId, actorId, viewId));

        Assert.assertNotNull(response);
        Assert.assertEquals(response.getStatusCode(), ViewResponse.StatusCode.OK);

        userVersion = getView(response, UserVersionsLatestView.class);
        Assert.assertNotNull(userVersion);

        version = userVersion.latest_authors();
        Assert.assertNotNull(version);
        Assert.assertEquals(version.body(), "body1");

        //delete latest and expose next latest
        write(EventBuilder.update(versionId2, tenantId, actorId).
            set(ReservedFields.DELETED, false).set("authors", Arrays.asList(userId1)).set("body", "body2").build());

        response = viewReader.readView(new ViewDescriptor(tenantIdAndCentricId, actorId, viewId));

        Assert.assertNotNull(response);
        Assert.assertEquals(response.getStatusCode(), ViewResponse.StatusCode.OK);

        userVersion = getView(response, UserVersionsLatestView.class);
        Assert.assertNotNull(userVersion);

        version = userVersion.latest_authors();
        Assert.assertNotNull(version);
        Assert.assertEquals(version.body(), "body2");

        //delete both
        write(EventBuilder.update(versionId1, tenantId, actorId).
            set(ReservedFields.DELETED, true).build());

        write(EventBuilder.update(versionId2, tenantId, actorId).
            set(ReservedFields.DELETED, true).build());

        response = viewReader.readView(new ViewDescriptor(tenantIdAndCentricId, actorId, viewId));

        Assert.assertNotNull(response);
        Assert.assertEquals(response.getStatusCode(), ViewResponse.StatusCode.OK);

        userVersion = getView(response, UserVersionsLatestView.class);
        Assert.assertNotNull(userVersion);

        version = userVersion.latest_authors();
        Assert.assertNull(version);

    }

    @Test
    public void testBackRefCount() throws Exception {
        //add
        String viewModel = "UserVersionsCountView::path1::User.count.Version.authors|Version.body";
        initModel(eventModel, viewModel);

        ObjectId userId1 = write(EventBuilder.create(idProvider, "User", tenantId, actorId).build());

        ObjectId versionId1 = write(EventBuilder.create(idProvider, "Version", tenantId, actorId).
            set("authors", Arrays.asList(userId1)).set("body", "body1").build());

        ViewId viewId = ViewId.ofId(userId1.getId(), UserVersionsCountView.class);
        ViewResponse response = viewReader.readView(new ViewDescriptor(tenantIdAndCentricId, actorId, viewId));

        Assert.assertNotNull(response);
        Assert.assertEquals(response.getStatusCode(), ViewResponse.StatusCode.OK);

        UserVersionsCountView userVersion = getView(response, UserVersionsCountView.class);
        Assert.assertNotNull(userVersion);


        Assert.assertEquals(userVersion.count_authors(), 1);

        ObjectId versionId2 = write(EventBuilder.create(idProvider, "Version", tenantId, actorId).
            set("authors", Arrays.asList(userId1)).set("body", "body2").build());

        response = viewReader.readView(new ViewDescriptor(tenantIdAndCentricId, actorId, viewId));

        Assert.assertNotNull(response);
        Assert.assertEquals(response.getStatusCode(), ViewResponse.StatusCode.OK);

        userVersion = getView(response, UserVersionsCountView.class);
        Assert.assertNotNull(userVersion);


        Assert.assertEquals(userVersion.count_authors(), 2);

        //clear one
        write(EventBuilder.update(versionId1, tenantId, actorId).
            clear("authors").build());

        ViewId.ofId(userId1.getId(), UserVersionsCountView.class);
        response = viewReader.readView(new ViewDescriptor(tenantIdAndCentricId, actorId, viewId));

        Assert.assertNotNull(response);
        Assert.assertEquals(response.getStatusCode(), ViewResponse.StatusCode.OK);

        userVersion = getView(response, UserVersionsCountView.class);
        Assert.assertNotNull(userVersion);

        Assert.assertEquals(userVersion.count_authors(), 1);

        //rereference
        write(EventBuilder.update(versionId1, tenantId, actorId).
            set("authors", Arrays.asList(userId1)).build());

        ViewId.ofId(userId1.getId(), UserVersionsCountView.class);
        response = viewReader.readView(new ViewDescriptor(tenantIdAndCentricId, actorId, viewId));

        Assert.assertNotNull(response);
        Assert.assertEquals(response.getStatusCode(), ViewResponse.StatusCode.OK);

        userVersion = getView(response, UserVersionsCountView.class);
        Assert.assertNotNull(userVersion);

        Assert.assertEquals(userVersion.count_authors(), 2);

        //delete one
        write(EventBuilder.update(versionId2, tenantId, actorId).
            set(ReservedFields.DELETED, true).build());

        ViewId.ofId(userId1.getId(), UserVersionsCountView.class);
        response = viewReader.readView(new ViewDescriptor(tenantIdAndCentricId, actorId, viewId));

        Assert.assertNotNull(response);
        Assert.assertEquals(response.getStatusCode(), ViewResponse.StatusCode.OK);

        userVersion = getView(response, UserVersionsCountView.class);
        Assert.assertNotNull(userVersion);

        Assert.assertEquals(userVersion.count_authors(), 1);

        //undelete
        write(EventBuilder.update(versionId2, tenantId, actorId).set(ReservedFields.DELETED, false).
            set("authors", Arrays.asList(userId1)).build());

        ViewId.ofId(userId1.getId(), UserVersionsCountView.class);
        response = viewReader.readView(new ViewDescriptor(tenantIdAndCentricId, actorId, viewId));

        Assert.assertNotNull(response);
        Assert.assertEquals(response.getStatusCode(), ViewResponse.StatusCode.OK);

        userVersion = getView(response, UserVersionsCountView.class);
        Assert.assertNotNull(userVersion);

        Assert.assertEquals(userVersion.count_authors(), 2);

        //delete both
        write(EventBuilder.update(versionId1, tenantId, actorId).
            set(ReservedFields.DELETED, true).build());

        write(EventBuilder.update(versionId2, tenantId, actorId).
            set(ReservedFields.DELETED, true).build());

        ViewId.ofId(userId1.getId(), UserVersionsCountView.class);
        response = viewReader.readView(new ViewDescriptor(tenantIdAndCentricId, actorId, viewId));

        Assert.assertNotNull(response);
        Assert.assertEquals(response.getStatusCode(), ViewResponse.StatusCode.OK);

        userVersion = getView(response, UserVersionsCountView.class);
        Assert.assertNotNull(userVersion);

        Assert.assertEquals(userVersion.count_authors(), 0);

    }
}
