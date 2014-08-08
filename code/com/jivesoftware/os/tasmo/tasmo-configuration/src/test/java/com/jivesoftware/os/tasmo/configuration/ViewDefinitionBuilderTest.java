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
package com.jivesoftware.os.tasmo.configuration;

import com.jivesoftware.os.tasmo.model.ViewBinding;
import com.jivesoftware.os.tasmo.model.path.ModelPath;
import com.jivesoftware.os.tasmo.model.path.ModelPathStep;
import com.jivesoftware.os.tasmo.model.path.ModelPathStepType;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Set;

/**
 *
 */
public class ViewDefinitionBuilderTest {

    private EventsModel eventsModel;

    @BeforeTest
    public void setUp() {
        String eventModel = "User:userName(value),creationDate(value)|Content:author(ref),subject(value),body(value)";
        eventsModel = new EventModelParser().parse(eventModel);
    }

    @Test
    public void testParseSimplePath() {
        String rootValues = "Content.subject,body";
        ViewDefinitionBuilder builder = new ViewDefinitionBuilder(eventsModel, "SimpleView", "Content");
        builder.addPath(rootValues);
        builder.setNotifiable(true);
        ViewBinding binding = builder.build();

        Assert.assertNotNull(binding);
        Assert.assertEquals(binding.getViewClassName(), "SimpleView");
        Assert.assertTrue(binding.isNotificationRequired());

        List<ModelPath> steps = binding.getModelPaths();
        Assert.assertNotNull(steps);
        Assert.assertEquals(steps.size(), 1);

        ModelPath path = steps.get(0);
        Assert.assertNotNull(path);
        Set<String> rootNames = path.getRootClassNames();
        Assert.assertNotNull(rootNames);
        Assert.assertEquals(rootNames.size(), 1);
        Assert.assertTrue(rootNames.contains("Content"));

        List<ModelPathStep> members = path.getPathMembers();
        Assert.assertNotNull(members);
        Assert.assertEquals(members.size(), 1);

        ModelPathStep step = members.get(0);
        Assert.assertNotNull(step);
        Assert.assertTrue(step.getIsRootId());
        Assert.assertTrue(step.getOriginClassNames().contains("Content"));
        Assert.assertNull(step.getRefFieldName());
        Assert.assertEquals(step.getFieldNames().size(), 2);
        Assert.assertTrue(step.getFieldNames().contains("subject"));
        Assert.assertTrue(step.getFieldNames().contains("body"));
        Assert.assertEquals(step.getStepType(), ModelPathStepType.value);

    }

    @Test
    public void testParseRefPath() {

        String rootValues = "Content.subject,body";
        String authorValues = "Content.author|User.userName";
        ViewDefinitionBuilder builder = new ViewDefinitionBuilder(eventsModel, "RefView", "Content");
        builder.addPath(rootValues);
        builder.addPath(authorValues);

        ViewBinding binding = builder.build();
        Assert.assertNotNull(binding);
        Assert.assertFalse(binding.isNotificationRequired());
        Assert.assertEquals(binding.getModelPaths().size(), 2);
        Assert.assertEquals(binding.getViewClassName(), "RefView");

        boolean found = false;
        for (ModelPath path : binding.getModelPaths()) {
            if (path.getPathMemberSize() == 2) {
                if (found) {
                    Assert.fail("Only one path should have 2 members");
                } else {
                    found = true;
                }

                List<ModelPathStep> steps = path.getPathMembers();
                ModelPathStep head = steps.get(0);
                Assert.assertTrue(head.getOriginClassNames().contains("Content"));
                Assert.assertTrue(head.getDestinationClassNames().contains("User"));
                Assert.assertEquals(head.getRefFieldName(), "author");
                Assert.assertEquals(head.getStepType(), ModelPathStepType.ref);
                Assert.assertTrue(head.getIsRootId());

                ModelPathStep tail = steps.get(1);
                Assert.assertFalse(tail.getIsRootId());
                Assert.assertTrue(tail.getOriginClassNames().contains("User"));
                Assert.assertNull(tail.getRefFieldName());
                Assert.assertEquals(tail.getFieldNames().size(), 1);
                Assert.assertTrue(tail.getFieldNames().contains("userName"));
                Assert.assertEquals(tail.getStepType(), ModelPathStepType.value);
            }
        }

        Assert.assertTrue(found);
    }

    @Test
    public void testParseBackRefPath() {
        String rootValues = "User.userName";
        String contentValues = "User.author(latest)|Content.body";
        ViewDefinitionBuilder builder = new ViewDefinitionBuilder(eventsModel, "BackRefView", "User");
        builder.addPath(rootValues);
        builder.addPath(contentValues);

        ViewBinding binding = builder.build();
        Assert.assertEquals(binding.getModelPaths().size(), 2);
        Assert.assertEquals(binding.getViewClassName(), "BackRefView");

        boolean found = false;
        for (ModelPath path : binding.getModelPaths()) {
            if (path.getPathMemberSize() == 2) {
                if (found) {
                    Assert.fail("Only one path should have 2 members");
                } else {
                    found = true;
                }

                List<ModelPathStep> steps = path.getPathMembers();
                ModelPathStep head = steps.get(0);
                Assert.assertTrue(head.getOriginClassNames().contains("Content"));
                Assert.assertTrue(head.getDestinationClassNames().contains("User"));
                Assert.assertEquals(head.getRefFieldName(), "author");
                Assert.assertEquals(head.getStepType(), ModelPathStepType.latest_backRef);
                Assert.assertTrue(head.getIsRootId());

                ModelPathStep tail = steps.get(1);
                Assert.assertFalse(tail.getIsRootId());
                Assert.assertTrue(tail.getOriginClassNames().contains("Content"));
                Assert.assertNull(tail.getRefFieldName());
                Assert.assertEquals(tail.getFieldNames().size(), 1);
                Assert.assertTrue(tail.getFieldNames().contains("body"));
                Assert.assertEquals(tail.getStepType(), ModelPathStepType.value);
            }
        }

        Assert.assertTrue(found);
    }
}
