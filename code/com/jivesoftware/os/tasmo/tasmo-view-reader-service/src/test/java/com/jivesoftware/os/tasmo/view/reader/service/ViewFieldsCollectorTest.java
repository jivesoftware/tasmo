package com.jivesoftware.os.tasmo.view.reader.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.jivesoftware.os.jive.utils.id.Id;
import com.jivesoftware.os.jive.utils.id.ObjectId;
import com.jivesoftware.os.jive.utils.id.TenantId;
import com.jivesoftware.os.tasmo.model.path.ModelPath;
import com.jivesoftware.os.tasmo.model.path.ModelPathStep;
import com.jivesoftware.os.tasmo.model.path.ModelPathStepType;
import com.jivesoftware.os.tasmo.view.reader.api.ViewDescriptor;
import com.jivesoftware.os.tasmo.view.reader.api.ViewResponse;
import com.jivesoftware.os.tasmo.view.reader.service.shared.ViewValue;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.google.common.collect.Sets.newHashSet;
import static com.jivesoftware.os.tasmo.model.path.ModelPathStepType.backRefs;
import static com.jivesoftware.os.tasmo.model.path.ModelPathStepType.count;
import static com.jivesoftware.os.tasmo.model.path.ModelPathStepType.latest_backRef;
import static com.jivesoftware.os.tasmo.model.path.ModelPathStepType.ref;
import static com.jivesoftware.os.tasmo.model.path.ModelPathStepType.refs;
import static com.jivesoftware.os.tasmo.model.path.ModelPathStepType.value;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;

public class ViewFieldsCollectorTest {

    private final ObjectMapper mapper = new ObjectMapper();
    private ViewFieldsCollector viewFieldsCollector;
    private ViewDescriptor viewDescriptor = new ViewDescriptor(new TenantId("tenant"), Id.NULL, new ObjectId("Booya", Id.NULL));

    private static Id id(int id) {
        return new Id(id);
    }

    @BeforeMethod
    public void setUpMethod() throws Exception {
        JsonViewMerger jsonViewMerger = new JsonViewMerger(mapper);
        viewFieldsCollector = new ViewFieldsCollector(jsonViewMerger, 1_024 * 1_024 * 10);
    }

    @AfterMethod
    public void tearDownMethod() throws Exception {
    }

    @Test
    public void testLacksPermissionCollector() throws Exception {

        ModelPath a = ModelPath.builder("Container.value").
                addPathMember(new ModelPathStep(true, newHashSet("Container"), null, value, null, Arrays.asList("name"))).build();

        viewFieldsCollector.add(viewDescriptor, a, new Id[]{new Id(1)}, new String[]{"Container"},
                new ViewValue(new long[]{1}, "{\"name\":\"bob\"}".getBytes()), 2L);
        viewFieldsCollector.done();

        Set<Id> permissions = new HashSet<>();
        ViewResponse view = viewFieldsCollector.getView(permissions);
        System.out.println("view=" + view);
        Assert.assertEquals(view.getStatusCode(), ViewResponse.StatusCode.FORBIDDEN);
    }

    @Test
    public void testHasPermissionsCollector() throws Exception {

        ModelPath a = ModelPath.builder("Container.value").
                addPathMember(new ModelPathStep(true, newHashSet("Container"), null, value, null, Arrays.asList("name"))).build();

        viewFieldsCollector.add(viewDescriptor, a, new Id[]{new Id(1)}, new String[]{"Container"},
                new ViewValue(new long[]{1}, "{\"name\":\"bob\"}".getBytes()), 2L);
        viewFieldsCollector.done();

        Set<Id> permisions = new HashSet<>();
        permisions.add(new Id(1L));
        ViewResponse view = viewFieldsCollector.getView(permisions);
        System.out.println("view=" + view);
        Assert.assertEquals(view.getStatusCode(), ViewResponse.StatusCode.OK);
        Assert.assertEquals(view.getViewBody().get("objectId").asText(), "Container_" + new Id(1).toStringForm());
        Assert.assertEquals(view.getViewBody().get("name").asText(), "bob");
    }

    @Test
    public void latestAffectedByPermissions() throws Exception {

        ModelPath a = ModelPath.builder("Document.date")
                .addPathMember(new ModelPathStep(true, singleton("Document"), "parent", latest_backRef, singleton("DocumentVersion"), null))
                .addPathMember(new ModelPathStep(false, singleton("DocumentVersion"), null, value, null, singletonList("date")))
                .build();

        viewFieldsCollector.add(viewDescriptor, a, new Id[]{new Id(1), new Id(2)}, new String[]{"Document", "DocumentVersion"},
                new ViewValue(new long[]{1, 2}, "{\"date\":\"January\"}".getBytes()), 1L);
        viewFieldsCollector.add(viewDescriptor, a, new Id[]{new Id(1), new Id(3)}, new String[]{"Document", "DocumentVersion"},
                new ViewValue(new long[]{1, 3}, "{\"date\":\"February\"}".getBytes()), 2L);
        viewFieldsCollector.add(viewDescriptor, a, new Id[]{new Id(1), new Id(4)}, new String[]{"Document", "DocumentVersion"},
                new ViewValue(new long[]{1, 4}, "{\"date\":\"March\"}".getBytes()), 3L);
        viewFieldsCollector.done();

        Set<Id> permisions = newHashSet(new Id(1L), new Id(2), new Id(3));
        ViewResponse view = viewFieldsCollector.getView(permisions);
        System.out.println("view=" + view);
        Assert.assertEquals(view.getStatusCode(), ViewResponse.StatusCode.OK);
        Assert.assertEquals(view.getViewBody().get("latest_parent").get("date").asText(), "February");
    }

    @Test
    public void latestCompletelyFilteredByPermissions() throws Exception {

        ModelPath a = ModelPath.builder("Document.date")
                .addPathMember(new ModelPathStep(true, singleton("Document"), "parent", latest_backRef, singleton("DocumentVersion"), null))
                .addPathMember(new ModelPathStep(false, singleton("DocumentVersion"), null, value, null, singletonList("date")))
                .build();

        viewFieldsCollector.add(viewDescriptor, a, new Id[]{new Id(1), new Id(2)}, new String[]{"Document", "DocumentVersion"},
                new ViewValue(new long[]{1, 2}, "{\"date\":\"January\"}".getBytes()), 1L);
        viewFieldsCollector.add(viewDescriptor, a, new Id[]{new Id(1), new Id(3)}, new String[]{"Document", "DocumentVersion"},
                new ViewValue(new long[]{1, 3}, "{\"date\":\"February\"}".getBytes()), 2L);
        viewFieldsCollector.add(viewDescriptor, a, new Id[]{new Id(1), new Id(4)}, new String[]{"Document", "DocumentVersion"},
                new ViewValue(new long[]{1, 4}, "{\"date\":\"March\"}".getBytes()), 3L);
        viewFieldsCollector.done();

        Set<Id> permisions = newHashSet(new Id(1L));
        ViewResponse view = viewFieldsCollector.getView(permisions);
        System.out.println("view=" + view);
        Assert.assertEquals(view.getStatusCode(), ViewResponse.StatusCode.OK);
        JsonNode latestParent = view.getViewBody().get("latest_parent");
        Assert.assertTrue(latestParent == null || latestParent.isNull());
    }

    @Test
    public void naturalOrderOfAdds() throws Exception {
        ModelPath c = ModelPath.builder("c")
                .addPathMember(new ModelPathStep(true, singleton("A"), "a2b", ref, singleton("B"), null))
                .addPathMember(new ModelPathStep(false, singleton("B"), "c2b", backRefs, singleton("C"), null))
                .addPathMember(new ModelPathStep(false, singleton("C"), null, value, null, singletonList("value")))
                .build();
        ModelPath b = ModelPath.builder("b")
                .addPathMember(new ModelPathStep(true, singleton("A"), "a2b", ref, singleton("B"), null))
                .addPathMember(new ModelPathStep(false, singleton("B"), null, value, null, singletonList("value")))
                .build();
        ModelPath a = ModelPath.builder("a")
                .addPathMember(new ModelPathStep(true, singleton("A"), null, value, null, singletonList("value")))
                .build();

        viewFieldsCollector.add(viewDescriptor, a, new Id[]{id(1)}, new String[]{"A"},
                new ViewValue(new long[]{1}, "{\"value\":\"value_a\"}".getBytes()), 1L);
        viewFieldsCollector.add(viewDescriptor, b, new Id[]{id(1), id(2)}, new String[]{"A", "B"},
                new ViewValue(new long[]{1, 2}, "{\"value\":\"value_b\"}".getBytes()), 2L);
        viewFieldsCollector.add(viewDescriptor, c, new Id[]{id(1), id(2), id(3)}, new String[]{"A", "B", "C"},
                new ViewValue(new long[]{1, 2, 3}, "{\"value\":\"value_c1\"}".getBytes()), 3L);
        viewFieldsCollector.add(viewDescriptor, c, new Id[]{id(1), id(2), id(4)}, new String[]{"A", "B", "C"},
                new ViewValue(new long[]{1, 2, 34}, "{\"value\":\"value_c2\"}".getBytes()), 4L);

        ViewResponse view = viewFieldsCollector.getView(newHashSet(id(1), id(2), id(3), id(4)));
        System.out.println(mapper.writeValueAsString(view.getViewBody()));
        Assert.assertEquals(view.getViewBody().get("value").asText(), "value_a");
        Assert.assertEquals(view.getViewBody().get("a2b").get("value").asText(), "value_b");
        JsonNode jsonNode = view.getViewBody().get("a2b").get("all_c2b");
        Assert.assertTrue(jsonNode.isArray());
        Set<String> values = new HashSet<>();
        for (JsonNode node : jsonNode) {
            values.add(node.get("value").asText());
        }
        Assert.assertEquals(values, newHashSet("value_c1", "value_c2"));
    }

    @Test
    public void reverseOrderOfAdds() throws Exception {
        ModelPath c = ModelPath.builder("c")
                .addPathMember(new ModelPathStep(true, singleton("A"), "a2b", ref, singleton("B"), null))
                .addPathMember(new ModelPathStep(false, singleton("B"), "c2b", backRefs, singleton("C"), null))
                .addPathMember(new ModelPathStep(false, singleton("C"), null, value, null, singletonList("value")))
                .build();
        ModelPath b = ModelPath.builder("b")
                .addPathMember(new ModelPathStep(true, singleton("A"), "a2b", ref, singleton("B"), null))
                .addPathMember(new ModelPathStep(false, singleton("B"), null, value, null, singletonList("value")))
                .build();
        ModelPath a = ModelPath.builder("a")
                .addPathMember(new ModelPathStep(true, singleton("A"), null, value, null, singletonList("value")))
                .build();

        viewFieldsCollector.add(viewDescriptor, c, new Id[]{id(1), id(2), id(4)}, new String[]{"A", "B", "C"},
                new ViewValue(new long[]{1, 2, 4}, "{\"value\":\"value_c2\"}".getBytes()), 4L);
        viewFieldsCollector.add(viewDescriptor, c, new Id[]{id(1), id(2), id(3)}, new String[]{"A", "B", "C"},
                new ViewValue(new long[]{1, 2, 3}, "{\"value\":\"value_c1\"}".getBytes()), 3L);
        viewFieldsCollector.add(viewDescriptor, b, new Id[]{id(1), id(2)}, new String[]{"A", "B"},
                new ViewValue(new long[]{1, 2}, "{\"value\":\"value_b\"}".getBytes()), 2L);
        viewFieldsCollector.add(viewDescriptor, a, new Id[]{id(1)}, new String[]{"A"},
                new ViewValue(new long[]{1}, "{\"value\":\"value_a\"}".getBytes()), 1L);

        ViewResponse view = viewFieldsCollector.getView(newHashSet(id(1), id(2), id(3), id(4)));
        System.out.println(mapper.writeValueAsString(view.getViewBody()));
        Assert.assertEquals(view.getViewBody().get("value").asText(), "value_a");
        Assert.assertEquals(view.getViewBody().get("a2b").get("value").asText(), "value_b");
        JsonNode jsonNode = view.getViewBody().get("a2b").get("all_c2b");
        Assert.assertTrue(jsonNode.isArray());
        Set<String> values = new HashSet<>();
        for (JsonNode node : jsonNode) {
            values.add(node.get("value").asText());
        }
        Assert.assertEquals(values, newHashSet("value_c1", "value_c2"));
    }

    @Test
    public void sameFieldDifferentType() throws Exception {
        ModelPath a = ModelPath.builder("a")
                .addPathMember(new ModelPathStep(true, singleton("A"), "b2a", backRefs, singleton("B"), null))
                .addPathMember(new ModelPathStep(false, singleton("B"), null, value, null, singletonList("value")))
                .build();
        ModelPath b = ModelPath.builder("b")
                .addPathMember(new ModelPathStep(true, singleton("A"), "b2a", latest_backRef, singleton("B"), null))
                .addPathMember(new ModelPathStep(false, singleton("B"), null, value, null, singletonList("value")))
                .build();
        ModelPath c = ModelPath.builder("c")
                .addPathMember(new ModelPathStep(true, singleton("A"), "b2a", count, singleton("B"), null))
                .addPathMember(new ModelPathStep(false, singleton("B"), null, value, null, singletonList("value")))
                .build();

        viewFieldsCollector.add(viewDescriptor, a, new Id[]{id(1), id(2)}, new String[]{"A", "B"},
                new ViewValue(new long[]{1, 2}, "{\"value\":\"value_b1\"}".getBytes()), 1L);
        viewFieldsCollector.add(viewDescriptor, a, new Id[]{id(1), id(3)}, new String[]{"A", "B"},
                new ViewValue(new long[]{1, 3}, "{\"value\":\"value_b2\"}".getBytes()), 2L);
        viewFieldsCollector.add(viewDescriptor, b, new Id[]{id(1), id(2)}, new String[]{"A", "B"},
                new ViewValue(new long[]{1, 2}, "{\"value\":\"value_b1\"}".getBytes()), 1L);
        viewFieldsCollector.add(viewDescriptor, b, new Id[]{id(1), id(3)}, new String[]{"A", "B"},
                new ViewValue(new long[]{1, 3}, "{\"value\":\"value_b2\"}".getBytes()), 2L);
        viewFieldsCollector.add(viewDescriptor, c, new Id[]{id(1), id(2)}, new String[]{"A", "B"},
                new ViewValue(new long[]{1, 2}, "{\"value\":\"value_b1\"}".getBytes()), 1L);
        viewFieldsCollector.add(viewDescriptor, c, new Id[]{id(1), id(3)}, new String[]{"A", "B"},
                new ViewValue(new long[]{1, 3}, "{\"value\":\"value_b2\"}".getBytes()), 2L);

        ViewResponse view = viewFieldsCollector.getView(newHashSet(id(1), id(2), id(3)));
        System.out.println(mapper.writeValueAsString(view.getViewBody()));
        JsonNode jsonNode = view.getViewBody().get("all_b2a");
        Assert.assertTrue(jsonNode.isArray());
        Set<String> values = new HashSet<>();
        for (JsonNode node : jsonNode) {
            values.add(node.get("value").asText());
        }
        Assert.assertEquals(values, newHashSet("value_b1", "value_b2"));
        Assert.assertEquals(view.getViewBody().get("latest_b2a").get("value").asText(), "value_b2");
        Assert.assertEquals(view.getViewBody().get("count_b2a").asInt(), 2);
    }

    @Test
    public void testCollector3() throws Exception {

        ModelPath a = ModelPath.builder("Container.parent.backrefs.Content.authors.refs.User.value").
                addPathMember(new ModelPathStep(true, newHashSet("Content"), "parent", backRefs, newHashSet("Container"), null)).
                addPathMember(new ModelPathStep(false, newHashSet("Content"), "authors", refs, newHashSet("User"), null)).
                addPathMember(new ModelPathStep(false, newHashSet("User"), null, value, null, Arrays.asList("name"))).build();

        viewFieldsCollector.add(viewDescriptor, a, new Id[]{new Id(1), new Id(2), new Id(3)}, new String[]{"Container", "Content", "User"},
                new ViewValue(new long[]{1, 2, 3}, "{\"name\":\"jane\"}".getBytes()), 1L);
        viewFieldsCollector.done();

        Set<Id> permissions = new HashSet<>();
        permissions.add(new Id(1L));
        permissions.add(new Id(2L));
        permissions.add(new Id(3L));
        ViewResponse viewResponse = viewFieldsCollector.getView(permissions);
        ObjectNode view = viewResponse.getViewBody();
        System.out.println("view=" + view);
        Assert.assertTrue(view.get("all_parent").isArray());
        Assert.assertTrue(view.get("all_parent").size() == 1);
        Assert.assertEquals(view.get("all_parent").get(0).get("objectId").asText(), "Content_" + new Id(2).toStringForm());
        Assert.assertTrue(view.get("all_parent").get(0).get("authors").isArray());
        Assert.assertTrue(view.get("all_parent").get(0).get("authors").size() == 1);
        Assert.assertEquals(view.get("all_parent").get(0).get("authors").get(0).get("objectId").asText(),
                "User_" + new Id(3).toStringForm());
        Assert.assertEquals(view.get("all_parent").get(0).get("authors").get(0).get("name").asText(), "jane");

    }

    @Test
    public void testCollector4() throws Exception {

        ModelPath a = ModelPath.builder("Container.parent.backrefs.Content.authors.refs.User.value").
                addPathMember(new ModelPathStep(true, newHashSet("Content"), "parent", backRefs, newHashSet("Container"), null)).
                addPathMember(new ModelPathStep(false, newHashSet("Content"), "authors", refs, newHashSet("User"), null)).
                addPathMember(new ModelPathStep(false, newHashSet("User"), null, value, null, Arrays.asList("name"))).build();

        viewFieldsCollector.add(viewDescriptor, a, new Id[]{new Id(1), new Id(2), new Id(3)}, new String[]{"Container", "Content", "User"},
                new ViewValue(new long[]{1, 2, 3}, "{\"name\":\"bob\"}".getBytes()), 1L);
        viewFieldsCollector.done();

        Set<Id> permissions = new HashSet<>();
        permissions.add(new Id(1L));
        permissions.add(new Id(3L));
        ViewResponse viewResponse = viewFieldsCollector.getView(permissions);
        ObjectNode view = viewResponse.getViewBody();
        System.out.println("view=" + view);
        Assert.assertTrue(view.get("all_parent").isArray());
        Assert.assertEquals(view.get("all_parent").size(), 0);
    }

    @Test
    public void testCollector5() throws Exception {

        ModelPath a = ModelPath.builder("Container.parent.backrefs.StatusUpdate|Document|Blog.name.value").
                addPathMember(new ModelPathStep(true, newHashSet("StatusUpdate", "Document", "Blog"), "parent",
                                backRefs, singleton("Container"), null)).
                addPathMember(new ModelPathStep(false, newHashSet("StatusUpdate", "Document", "Blog"), null,
                                value, null, Arrays.asList("name")))
                .build();

        viewFieldsCollector.add(viewDescriptor, a, new Id[]{new Id(1), new Id(2)}, new String[]{"Container", "Document"},
                new ViewValue(new long[]{1, 2}, "{\"name\":\"test1\"}".getBytes()), 1L);
        viewFieldsCollector.add(viewDescriptor, a, new Id[]{new Id(1), new Id(3)}, new String[]{"Container", "StatusUpdate"},
                new ViewValue(new long[]{1, 3}, "{\"name\":\"test2\"}".getBytes()), 2L);
        viewFieldsCollector.add(viewDescriptor, a, new Id[]{new Id(1), new Id(4)}, new String[]{"Container", "Document"},
                new ViewValue(new long[]{1, 4}, "{\"name\":\"test1\"}".getBytes()), 1L);
        viewFieldsCollector.add(viewDescriptor, a, new Id[]{new Id(1), new Id(5)}, new String[]{"Container", "Document"},
                new ViewValue(new long[]{1, 5}, "{\"name\":\"test1\"}".getBytes()), 1L);
        viewFieldsCollector.done();

        ViewResponse viewResponse = viewFieldsCollector.getView(newHashSet(id(1), id(2), id(3), id(4)));
        ObjectNode view = viewResponse.getViewBody();
        System.out.println("view=" + view);
        JsonNode allParent = view.get("all_parent");
        Assert.assertTrue(allParent.isArray());
        Set<String> ids = new HashSet<>();
        for (JsonNode node : allParent) {
            ids.add(node.get("objectId").asText());
        }
        Assert.assertEquals(ids, newHashSet("Document_" + new Id(2).toStringForm(), "StatusUpdate_" + new Id(3).toStringForm(),
                "Document_" + new Id(4).toStringForm()));

    }

    @Test
    public void testCountCollector() throws Exception {

        ModelPath a = ModelPath.builder("Container.parent.count.StatusUpdate|Document|Blog.name.value")
                .addPathMember(new ModelPathStep(true, newHashSet("StatusUpdate", "Document", "Blog"), "parent",
                                ModelPathStepType.count, newHashSet("Container"), null))
                .addPathMember(new ModelPathStep(false, newHashSet("StatusUpdate", "Document", "Blog"), null,
                                value, null, Arrays.asList("instanceId")))
                .build();

        viewFieldsCollector.add(viewDescriptor, a, new Id[]{new Id(1), new Id(2)}, new String[]{"Container", "Document"},
                new ViewValue(new long[]{1, 2}, "{\"instanceId\":11}".getBytes()), 1L);
        viewFieldsCollector.add(viewDescriptor, a, new Id[]{new Id(1), new Id(3)}, new String[]{"Container", "StatusUpdate"},
                new ViewValue(new long[]{1, 3}, "{\"instanceId\":12}".getBytes()), 2L);
        viewFieldsCollector.add(viewDescriptor, a, new Id[]{new Id(1), new Id(4)}, new String[]{"Container", "Document"},
                new ViewValue(new long[]{1, 4}, "{\"instanceId\":13}".getBytes()), 1L);
        viewFieldsCollector.add(viewDescriptor, a, new Id[]{new Id(1), new Id(5)}, new String[]{"Container", "Document"},
                new ViewValue(new long[]{1, 5}, "{\"instanceId\":14}".getBytes()), 1L);
        viewFieldsCollector.done();

        Set<Id> permissions = new HashSet<>();
        permissions.add(new Id(1L));
        permissions.add(new Id(2L));
        permissions.add(new Id(3L));
        permissions.add(new Id(4L));
        permissions.add(new Id(5L));
        ViewResponse viewResponse = viewFieldsCollector.getView(permissions);
        ObjectNode view = viewResponse.getViewBody();
        System.out.println("view=" + view);
        Assert.assertTrue(view.get("count_parent").isInt());
        Assert.assertEquals(view.get("count_parent").asInt(), 4);
    }

    @Test
    public void testLatestBackref() throws Exception {
        ModelPath a = ModelPath.builder("Container.parent.latset_backref.StatusUpdate|Document|Blog.name.value")
                .addPathMember(new ModelPathStep(true, newHashSet("StatusUpdate", "Document", "Blog"), "parent",
                                latest_backRef, newHashSet("Container"), null))
                .addPathMember(new ModelPathStep(false, newHashSet("StatusUpdate", "Document", "Blog"), null,
                                value, null, Arrays.asList("instanceId")))
                .build();

        Set<Id> permissions = new HashSet<>();
        ViewResponse viewResponse = viewFieldsCollector.getView(permissions);
        ObjectNode view = viewResponse.getViewBody();
        Assert.assertNull(view);

        viewFieldsCollector.add(viewDescriptor, a, new Id[]{new Id(1), new Id(2)}, new String[]{"Container", "Document"},
                new ViewValue(new long[]{1, 2}, "{\"instanceId\":11}".getBytes()), 1L);
        viewFieldsCollector.add(viewDescriptor, a, new Id[]{new Id(1), new Id(3)}, new String[]{"Container", "StatusUpdate"},
                new ViewValue(new long[]{1, 3}, "{\"instanceId\":12}".getBytes()), 2L);
        viewFieldsCollector.add(viewDescriptor, a, new Id[]{new Id(1), new Id(4)}, new String[]{"Container", "Document"},
                new ViewValue(new long[]{1, 5}, "{\"instanceId\":13}".getBytes()), 4L);
        viewFieldsCollector.add(viewDescriptor, a, new Id[]{new Id(1), new Id(5)}, new String[]{"Container", "Document"},
                new ViewValue(new long[]{1, 4}, "{\"instanceId\":14}".getBytes()), 3L);
        viewFieldsCollector.done();

        viewResponse = viewFieldsCollector.getView(permissions);
        view = viewResponse.getViewBody();
        Assert.assertNull(view);

        permissions.add(new Id(1L));
        permissions.add(new Id(2L));
        permissions.add(new Id(3L));
        permissions.add(new Id(4L));
        permissions.add(new Id(5L));

        viewResponse = viewFieldsCollector.getView(permissions);
        view = viewResponse.getViewBody();
        System.out.println("view=" + view);
        JsonNode got = view.get("latest_parent");
        Assert.assertTrue(got.isObject());
        JsonNode field = got.get("instanceId");
        Assert.assertTrue(field.isInt());
        Assert.assertEquals(field.asInt(), 13); //added at the highest timestamp
    }
}
