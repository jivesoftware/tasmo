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
import com.jivesoftware.os.tasmo.id.ObjectId;
import com.jivesoftware.os.tasmo.view.reader.api.ViewDescriptor;
import com.jivesoftware.os.tasmo.view.reader.api.ViewId;
import com.jivesoftware.os.tasmo.view.reader.api.ViewResponse;
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
    
    String eventModel =
        "User:userName(value),creationDate(value),manager(ref)|Content:location(ref)|Version:parent(ref),authors(refs),subject(value),body(value)|" +
        "Container:name(value),creationDate(value),owner(ref)";

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
        
        ContentView content = response.getView(ContentView.class);
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
        
        content = response.getView(ContentView.class);
        Assert.assertNotNull(content);
        
        container = content.location();
        Assert.assertNotNull(container);
        
        Assert.assertEquals(container.name(), "not awesome");
        Assert.assertEquals(container.creationDate(), (Long)12345l);
        
        write(EventBuilder.update(containerId, tenantId, actorId).clear("name").build());
        
        response = viewReader.readView(new ViewDescriptor(tenantIdAndCentricId, actorId, viewId));
        
        Assert.assertNotNull(response);
        Assert.assertEquals(response.getStatusCode(), ViewResponse.StatusCode.OK);
        
        content = response.getView(ContentView.class);
        Assert.assertNotNull(content);
        
        container = content.location();
        Assert.assertNotNull(container);
        
        Assert.assertNull(container.name());
        Assert.assertEquals(container.creationDate(), (Long)12345l);
        
        //clear value
        write(EventBuilder.update(containerId, tenantId, actorId).clear("name").build());
        
        response = viewReader.readView(new ViewDescriptor(tenantIdAndCentricId, actorId, viewId));
        
        Assert.assertNotNull(response);
        Assert.assertEquals(response.getStatusCode(), ViewResponse.StatusCode.OK);
        
        content = response.getView(ContentView.class);
        Assert.assertNotNull(content);
        
        container = content.location();
        Assert.assertNotNull(container);
        
        Assert.assertNull(container.name());
        Assert.assertEquals(container.creationDate(), (Long)12345l);
        
        //dereference
        write(EventBuilder.update(contentId, tenantId, actorId).clear("location").build());
        
        response = viewReader.readView(new ViewDescriptor(tenantIdAndCentricId, actorId, viewId));
        
        Assert.assertNotNull(response);
        Assert.assertEquals(response.getStatusCode(), ViewResponse.StatusCode.OK);
        
        content = response.getView(ContentView.class);
        Assert.assertNotNull(content);
        
        container = content.location();
        Assert.assertNull(container);
    
        //re-reference
        write(EventBuilder.update(contentId, tenantId, actorId).set("location", containerId).build());
        
        response = viewReader.readView(new ViewDescriptor(tenantIdAndCentricId, actorId, viewId));
        
        Assert.assertNotNull(response);
        Assert.assertEquals(response.getStatusCode(), ViewResponse.StatusCode.OK);
        
        content = response.getView(ContentView.class);
        Assert.assertNotNull(content);
        
        container = content.location();
        Assert.assertNotNull(container);
        
        Assert.assertNull(container.name());
        Assert.assertEquals(container.creationDate(), (Long)12345l);
        
        //delete value
        write(EventBuilder.update(containerId, tenantId, actorId).set(ReservedFields.DELETED, true).build());
        response = viewReader.readView(new ViewDescriptor(tenantIdAndCentricId, actorId, viewId));
        
        Assert.assertNotNull(response);
        Assert.assertEquals(response.getStatusCode(), ViewResponse.StatusCode.OK);
        
        content = response.getView(ContentView.class);
        Assert.assertNotNull(content);
        
        container = content.location();
        Assert.assertNull(container);
        
      
        //undelete
        write(EventBuilder.update(containerId, tenantId, actorId).set(ReservedFields.DELETED, false).build());
        
        response = viewReader.readView(new ViewDescriptor(tenantIdAndCentricId, actorId, viewId));
        
        Assert.assertNotNull(response);
        Assert.assertEquals(response.getStatusCode(), ViewResponse.StatusCode.OK);
        
        content = response.getView(ContentView.class);
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
        
        content = response.getView(ContentView.class);
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
    public void testMultiRefToValue() {
        
    }
    
    @Test
    public void testBackRefsToValue() {
        
    }
    
    @Test
    public void testLatestBackRefToValue() {
        
    }
    
    @Test
    public void testBackRefCount() {
        
    }
    
}
