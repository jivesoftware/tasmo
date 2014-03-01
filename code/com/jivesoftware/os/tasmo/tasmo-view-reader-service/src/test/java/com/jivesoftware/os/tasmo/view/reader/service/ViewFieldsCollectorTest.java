package com.jivesoftware.os.tasmo.view.reader.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Sets;
import com.jivesoftware.os.tasmo.id.Id;
import com.jivesoftware.os.tasmo.model.path.ModelPath;
import com.jivesoftware.os.tasmo.model.path.ModelPathStep;
import com.jivesoftware.os.tasmo.model.path.ModelPathStepType;
import com.jivesoftware.os.tasmo.view.reader.api.ViewResponse;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ViewFieldsCollectorTest {

    private final ObjectMapper mapper = new ObjectMapper();
    private ViewFieldsCollector viewFieldsCollector;

    @BeforeMethod
    public void setUpMethod() throws Exception {
        JsonViewMerger jsonViewMerger = new JsonViewMerger(mapper);
        viewFieldsCollector = new ViewFieldsCollector(jsonViewMerger);
    }

    @AfterMethod
    public void tearDownMethod() throws Exception {
    }

    @Test
    public void testLacksPermissionCollector() throws Exception {

        ModelPath a = ModelPath.builder("Container.value").
                addPathMember(new ModelPathStep(true, Sets.newHashSet("Container"), null, ModelPathStepType.value, null, Arrays.asList("name"))).build();

        viewFieldsCollector.add(a, new Id[]{new Id(1)}, new String[]{"Container"}, "{\"name\":\"bob\"}", 2L);
        viewFieldsCollector.done();

        Set<Id> permissions = new HashSet<>();
        ViewResponse view = viewFieldsCollector.getView(permissions);
        System.out.println("view=" + view);
        Assert.assertEquals(view.getStatusCode(), ViewResponse.StatusCode.FORBIDDEN);
    }

    @Test
    public void testHasPermissionsCollector() throws Exception {

        ModelPath a = ModelPath.builder("Container.value").
                addPathMember(new ModelPathStep(true, Sets.newHashSet("Container"), null, ModelPathStepType.value, null, Arrays.asList("name"))).build();

        viewFieldsCollector.add(a, new Id[]{new Id(1)}, new String[]{"Container"}, "{\"name\":\"bob\"}", 2L);
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
    public void testCollector3() throws Exception {

        ModelPath a = ModelPath.builder("Container.parent.backrefs.Content.authors.refs.User.value").
                addPathMember(new ModelPathStep(true, Sets.newHashSet("Content"), "parent", ModelPathStepType.backRefs, Sets.newHashSet("Container"), null)).
                addPathMember(new ModelPathStep(false, Sets.newHashSet("Content"), "authors", ModelPathStepType.refs, Sets.newHashSet("User"), null)).
                addPathMember(new ModelPathStep(false, Sets.newHashSet("User"), null, ModelPathStepType.value, null, Arrays.asList("name"))).build();


        viewFieldsCollector.add(a, new Id[]{new Id(1), new Id(2), new Id(3)}, new String[]{"Container", "Content", "User"},
                "{\"name\":\"jane\"}", 1L);
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
                addPathMember(new ModelPathStep(true, Sets.newHashSet("Content"), "parent", ModelPathStepType.backRefs, Sets.newHashSet("Container"), null)).
                addPathMember(new ModelPathStep(false, Sets.newHashSet("Content"), "authors", ModelPathStepType.refs, Sets.newHashSet("User"), null)).
                addPathMember(new ModelPathStep(false, Sets.newHashSet("User"), null, ModelPathStepType.value, null, Arrays.asList("name"))).build();


        viewFieldsCollector.add(a, new Id[]{new Id(1), new Id(2), new Id(3)}, new String[]{"Container", "Content", "User"},
                "{\"name\":\"bob\"}", 1L);
        viewFieldsCollector.done();

        Set<Id> permissions = new HashSet<>();
        permissions.add(new Id(1L));
        permissions.add(new Id(3L));
        ViewResponse viewResponse = viewFieldsCollector.getView(permissions);
        ObjectNode view = viewResponse.getViewBody();
        System.out.println("view=" + view);
        Assert.assertTrue(view.get("all_parent").isArray());
        Assert.assertEquals(view.get("all_parent").size(), 0);
        /*
         Assert.assertEquals(view.get("all_parent").get(0).get("objectId").asText(), "Content_" + new Id(2).toStringForm());
         Assert.assertTrue(view.get("all_parent").get(0).get("authors").isArray());
         Assert.assertTrue(view.get("all_parent").get(0).get("authors").size() == 1);
         Assert.assertEquals(view.get("all_parent").get(0).get("authors").get(0).get("objectId").asText(), "User_" + new Id(3).toStringForm());
         Assert.assertEquals(view.get("all_parent").get(0).get("authors").get(0).get("name"), null);
         */
    }

    @Test
    public void testCollector5() throws Exception {

        ModelPath a = ModelPath.builder("Container.parent.backrefs.StatusUpdate|Document|Blog.name.value").
                addPathMember(new ModelPathStep(true, Sets.newHashSet("StatusUpdate", "Document", "Blog"), "parent",
                ModelPathStepType.backRefs, Sets.newHashSet("Container"), null)).
                addPathMember(new ModelPathStep(false, Sets.newHashSet("StatusUpdate", "Document", "Blog"), null,
                ModelPathStepType.value, null, Arrays.asList("name")))
                .build();


        viewFieldsCollector.add(a, new Id[]{new Id(1), new Id(2)}, new String[]{"Container", "Document"},
                "{\"name\":\"test1\"}", 1L);
        viewFieldsCollector.add(a, new Id[]{new Id(1), new Id(3)}, new String[]{"Container", "StatusUpdate"},
                "{\"name\":\"test2\"}", 2L);
        viewFieldsCollector.add(a, new Id[]{new Id(1), new Id(4)}, new String[]{"Container", "Document"},
                "{\"name\":\"test1\"}", 1L);
        viewFieldsCollector.add(a, new Id[]{new Id(1), new Id(5)}, new String[]{"Container", "Document"},
                "{\"name\":\"test1\"}", 1L);
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
        Assert.assertTrue(view.get("all_parent").isArray());
        Assert.assertTrue(view.get("all_parent").size() == 4);
        Assert.assertEquals(view.get("all_parent").get(0).get("objectId").asText(), "Document_" + new Id(2).toStringForm());
        Assert.assertEquals(view.get("all_parent").get(1).get("objectId").asText(), "StatusUpdate_" + new Id(3).toStringForm());
        Assert.assertEquals(view.get("all_parent").get(2).get("objectId").asText(), "Document_" + new Id(4).toStringForm());
        Assert.assertEquals(view.get("all_parent").get(3).get("objectId").asText(), "Document_" + new Id(5).toStringForm());

    }

    @Test
    public void testCountCollector() throws Exception {

        ModelPath a = ModelPath.builder("Container.parent.count.StatusUpdate|Document|Blog.name.value")
                .addPathMember(new ModelPathStep(true, Sets.newHashSet("StatusUpdate", "Document", "Blog"), "parent",
                ModelPathStepType.count, Sets.newHashSet("Container"), null))
                .addPathMember(new ModelPathStep(false, Sets.newHashSet("StatusUpdate", "Document", "Blog"), null,
                ModelPathStepType.value, null, Arrays.asList("instanceId")))
                .build();


        viewFieldsCollector.add(a, new Id[]{new Id(1), new Id(2)}, new String[]{"Container", "Document"},
                "{\"instanceId\":11}", 1L);
        viewFieldsCollector.add(a, new Id[]{new Id(1), new Id(3)}, new String[]{"Container", "StatusUpdate"},
                "{\"instanceId\":12}", 2L);
        viewFieldsCollector.add(a, new Id[]{new Id(1), new Id(4)}, new String[]{"Container", "Document"},
                "{\"instanceId\":13}", 1L);
        viewFieldsCollector.add(a, new Id[]{new Id(1), new Id(5)}, new String[]{"Container", "Document"},
                "{\"instanceId\":14}", 1L);
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
    public void testLatestBackref() throws IOException, Exception {
        ModelPath a = ModelPath.builder("Container.parent.latset_backref.StatusUpdate|Document|Blog.name.value")
                .addPathMember(new ModelPathStep(true, Sets.newHashSet("StatusUpdate", "Document", "Blog"), "parent",
                ModelPathStepType.latest_backRef, Sets.newHashSet("Container"), null))
                .addPathMember(new ModelPathStep(false, Sets.newHashSet("StatusUpdate", "Document", "Blog"), null,
                ModelPathStepType.value, null, Arrays.asList("instanceId")))
                .build();

        Set<Id> permissions = new HashSet<>();
        ViewResponse viewResponse = viewFieldsCollector.getView(permissions);
        ObjectNode view = viewResponse.getViewBody();
        Assert.assertNull(view);

        viewFieldsCollector.add(a, new Id[]{new Id(1), new Id(2)}, new String[]{"Container", "Document"},
                "{\"instanceId\":11}", 1L);
        viewFieldsCollector.add(a, new Id[]{new Id(1), new Id(3)}, new String[]{"Container", "StatusUpdate"},
                "{\"instanceId\":12}", 2L);
        viewFieldsCollector.add(a, new Id[]{new Id(1), new Id(4)}, new String[]{"Container", "Document"},
                "{\"instanceId\":13}", 4L);
        viewFieldsCollector.add(a, new Id[]{new Id(1), new Id(5)}, new String[]{"Container", "Document"},
                "{\"instanceId\":14}", 3L);
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
        JsonNode field = ((ObjectNode) got).get("instanceId");
        Assert.assertTrue(field.isInt());
        Assert.assertEquals(field.asInt(), 13); //added at the highest timestamp
    }
}
