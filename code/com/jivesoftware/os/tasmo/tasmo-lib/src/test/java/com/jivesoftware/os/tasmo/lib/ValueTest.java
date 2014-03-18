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
package com.jivesoftware.os.tasmo.lib;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.jivesoftware.os.tasmo.event.api.ReservedFields;
import com.jivesoftware.os.tasmo.event.api.write.EventBuilder;
import com.jivesoftware.os.tasmo.id.Id;
import com.jivesoftware.os.tasmo.id.ObjectId;
import java.util.Arrays;
import java.util.Date;
import org.testng.annotations.Test;

/**
 *
 * @author pete
 */
public class ValueTest extends BaseTasmoTest {

    @Test
    public void testSetAndReassignValues() throws Exception {

        Expectations expectations = initEventModel(
            "User:username(value),firstName(value),lastName(value),creationDate(value),tags(value),content(refs),manager(ref),complexObject(value)");

        ObjectId content1 = new ObjectId("Content", new Id(orderIdProvider.nextId()));
        ObjectId content2 = new ObjectId("Content", new Id(orderIdProvider.nextId()));
        ObjectId manager1 = new ObjectId("User", new Id(orderIdProvider.nextId()));

        ObjectNode complexObject = mapper.createObjectNode();
        complexObject.put("decimal", 0.23);
        complexObject.put("string", "I am a string");
        complexObject.put("bytearray", new byte[]{0, 56, 100});


        ObjectId user1 = write(EventBuilder.create(idProvider, "User", tenantId, actorId).set("userName", "jjaneson").
            set("firstName", "jane").set("lastName", "janeson").build());

        expectations.addValueExpectation(user1, Arrays.asList("userName", "firstName", "lastName", "creationDate", "tags",
            "content", "manager", "complexObject"),
            Arrays.<Object>asList("jjaneson", "jane", "janeson", null, null, null, null, null));

        expectations.addExistenceExpectation(user1, true);
        expectations.assertExpectation(tenantIdAndCentricId);
        expectations.clear();


        write(EventBuilder.update(user1, tenantId, actorId).set("creationDate", new Date(12345)).
            set("tags", Arrays.asList("tag1", "tag2", "tag3")).set("content", Arrays.asList(content1, content2)).
            set("manager", manager1).set("complexObject", complexObject).build());

        expectations.addValueExpectation(user1, Arrays.asList("userName", "firstName", "lastName", "creationDate", "tags",
            "content", "manager", "complexObject"),
            Arrays.<Object>asList("jjaneson", "jane", "janeson", new Date(12345), Arrays.asList("tag1", "tag2", "tag3"),
            Arrays.asList(content1, content2), manager1, complexObject));

        expectations.assertExpectation(tenantIdAndCentricId);
        expectations.clear();

        complexObject.put("int", 23);
        complexObject.remove("decimal");

        write(EventBuilder.update(user1, tenantId, actorId).set("userName", "jjohnson").
            set("firstName", "john").set("lastName", "johnson").set("creationDate", new Date(54321)).
            set("tags", Arrays.asList("tag4")).set("content", Arrays.asList(content2)).
            set("manager", user1).set("complexObject", complexObject).build());

        expectations.addValueExpectation(user1, Arrays.asList("userName", "firstName", "lastName", "creationDate", "tags",
            "content", "manager", "complexObject"),
            Arrays.<Object>asList("jjohnson", "john", "johnson", new Date(54321), Arrays.asList("tag4"),
            Arrays.asList(content2), user1, complexObject));

        expectations.assertExpectation(tenantIdAndCentricId);
        expectations.clear();

        expectations.addValueExpectation(user1, Arrays.asList(ReservedFields.INSTANCE_ID), Arrays.<Object>asList(user1.getId()));
        expectations.assertExpectation(tenantIdAndCentricId);
        expectations.clear();

    }

    @Test
    public void testDeletion() throws Exception {
        Expectations expectations = initEventModel(
            "User:username(value),firstName(value),lastName(value),creationDate(value),tags(value),content(refs),manager(ref),complexObject(value)");

        ObjectId content1 = new ObjectId("Content", new Id(orderIdProvider.nextId()));
        ObjectId content2 = new ObjectId("Content", new Id(orderIdProvider.nextId()));
        ObjectId manager1 = new ObjectId("User", new Id(orderIdProvider.nextId()));

        ObjectNode complexObject = mapper.createObjectNode();
        complexObject.put("decimal", 0.23);
        complexObject.put("string", "I am a string");
        complexObject.put("bytearray", new byte[]{0, 56, 100});

        ObjectId user1 = write(EventBuilder.create(idProvider, "User", tenantId, actorId).set("userName", "jjaneson").
            set("firstName", "jane").set("lastName", "janeson").set("creationDate", new Date(12345)).
            set("tags", Arrays.asList("tag1", "tag2", "tag3")).set("content", Arrays.asList(content1, content2)).
            set("manager", manager1).set("complexObject", complexObject).build());

        expectations.addValueExpectation(user1, Arrays.asList("userName", "firstName", "lastName", "creationDate", "tags",
            "content", "manager", "complexObject"),
            Arrays.<Object>asList("jjaneson", "jane", "janeson", new Date(12345), Arrays.asList("tag1", "tag2", "tag3"),
            Arrays.asList(content1, content2), manager1, complexObject));

        expectations.addExistenceExpectation(user1, true);
        expectations.assertExpectation(tenantIdAndCentricId);
        expectations.clear();

        write(EventBuilder.update(user1, tenantId, actorId).set(ReservedFields.DELETED, true).build());
        expectations.addValueExpectation(user1, Arrays.asList("userName", "firstName", "lastName", "creationDate", "tags",
            "content", "manager", "complexObject"),
            Arrays.<Object>asList(null, null, null, null, null, null, null, null));
        expectations.addExistenceExpectation(user1, false);
        expectations.assertExpectation(tenantIdAndCentricId);
        expectations.clear();

        expectations.addValueExpectation(user1, Arrays.asList(ReservedFields.INSTANCE_ID), Arrays.<Object>asList((Object) null));
        expectations.assertExpectation(tenantIdAndCentricId);
        expectations.clear();
    }

    @Test
    public void testClear() throws Exception {
        Expectations expectations = initEventModel(
            "User:username(value),firstName(value),lastName(value),creationDate(value),tags(value),content(refs),manager(ref),complexObject(value)");

        ObjectId content1 = new ObjectId("Content", new Id(orderIdProvider.nextId()));
        ObjectId content2 = new ObjectId("Content", new Id(orderIdProvider.nextId()));
        ObjectId manager1 = new ObjectId("User", new Id(orderIdProvider.nextId()));

        ObjectNode complexObject = mapper.createObjectNode();
        complexObject.put("decimal", 0.23);
        complexObject.put("string", "I am a string");
        complexObject.put("bytearray", new byte[]{0, 56, 100});

        ObjectId user1 = write(EventBuilder.create(idProvider, "User", tenantId, actorId).set("userName", "jjaneson").
            set("firstName", "jane").set("lastName", "janeson").set("creationDate", new Date(12345)).
            set("tags", Arrays.asList("tag1", "tag2", "tag3")).set("content", Arrays.asList(content1, content2)).
            set("manager", manager1).set("complexObject", complexObject).build());

        expectations.addValueExpectation(user1, Arrays.asList("userName", "firstName", "lastName", "creationDate", "tags",
            "content", "manager", "complexObject"),
            Arrays.<Object>asList("jjaneson", "jane", "janeson", new Date(12345), Arrays.asList("tag1", "tag2", "tag3"),
            Arrays.asList(content1, content2), manager1, complexObject));

        expectations.addExistenceExpectation(user1, true);
        expectations.assertExpectation(tenantIdAndCentricId);
        expectations.clear();

        write(EventBuilder.update(user1, tenantId, actorId).clear("userName").clear("firstName").clear("lastName").clear("creationDate").
            clear("tags").build());

        expectations.addValueExpectation(user1, Arrays.asList("userName", "firstName", "lastName", "creationDate", "tags",
            "content", "manager", "complexObject"),
            Arrays.<Object>asList(null, null, null, null, null, Arrays.asList(content1, content2), manager1, complexObject));
        expectations.addExistenceExpectation(user1, true);
        expectations.assertExpectation(tenantIdAndCentricId);
        expectations.clear();


        write(EventBuilder.update(user1, tenantId, actorId).clear("content").clear("manager").clear("complexObject").build());

        expectations.addValueExpectation(user1, Arrays.asList("userName", "firstName", "lastName", "creationDate", "tags",
            "content", "manager", "complexObject"),
            Arrays.<Object>asList(null, null, null, null, null, null, null, null));
        expectations.addExistenceExpectation(user1, true);
        expectations.assertExpectation(tenantIdAndCentricId);
        expectations.clear();

        expectations.addValueExpectation(user1, Arrays.asList(ReservedFields.INSTANCE_ID), Arrays.<Object>asList(user1.getId()));
        expectations.assertExpectation(tenantIdAndCentricId);
        expectations.clear();

    }
}
