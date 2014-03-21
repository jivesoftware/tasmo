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

import com.jivesoftware.os.tasmo.event.api.write.EventBuilder;
import com.jivesoftware.os.tasmo.id.BaseView;
import com.jivesoftware.os.tasmo.id.ObjectId;
import com.jivesoftware.os.tasmo.view.reader.api.ViewDescriptor;
import com.jivesoftware.os.tasmo.view.reader.api.ViewResponse;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 *
 * @author pete
 */
public class AddsOnlyTest extends BaseTasmoViewTest {
    //VersionView::path1::Version.backRefs.Content.ref_versionedContent|Content.ref_originalAuthor.ref.User|User.firstName
    String eventModel =
        "User:userName(value),creationDate(value),manager(ref)|Content:location(ref)|Version:parent(ref),authors(refs),subject(value),body(value)";
    
    private static interface Version extends BaseView {
        
        String subject();
        
        String body();
        
    };
    
    @Test
    public void testValueOnly() throws Exception {
        String viewModel = "VersionView::path1::Version.subject,body";
        initModel(eventModel, viewModel);
        
        ObjectId objectId = write(EventBuilder.create(idProvider, "Version", tenantId, actorId, actorId).set("subject", "awesome").build());
        ViewResponse response = viewReader.readView(new ViewDescriptor(tenantId, actorId, new ObjectId("VersionView", objectId.getId())));
        
        Assert.assertNotNull(response);
        Assert.assertEquals(response.getStatusCode(), ViewResponse.StatusCode.OK);
        
        Version version = response.getView(Version.class);
        
        Assert.assertNotNull(version);
        Assert.assertEquals("awesome", version.subject());
    }
    
    
    @Test
    public void testSingleRefValue() {
        
    }
    
    @Test
    public void testMultiRefValue() {
        
    }
    
    @Test
    public void testLatestBackRefValue() {
        
    }
    
    @Test
    public void testAllBackRefValue() {
        
    }
    
    @Test
    public void testCount() {
    }
    
    @Test
    public void testAllOneAndTwoStepPaths() {
        
    }
    
    @Test
    public void testSingleRefSingleRefValue() {
        
    }
    
    @Test
    public void testMultiRefMultiRefValue() {
        
    }
    
    @Test
    public void testLatestBackRefLatestBackRefValue() {
        
    }
    
    @Test
    public void testAllBackRefAllBackRefValue() {
        
    }
    
    @Test
    public void testAllThreeStepPaths() {
        
    }
}
