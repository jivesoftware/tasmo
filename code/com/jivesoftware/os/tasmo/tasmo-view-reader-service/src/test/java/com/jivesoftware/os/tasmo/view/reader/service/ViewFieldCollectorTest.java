/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.jivesoftware.os.tasmo.view.reader.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Sets;
import com.jivesoftware.os.tasmo.id.Id;
import com.jivesoftware.os.tasmo.id.ObjectId;
import com.jivesoftware.os.tasmo.model.path.ModelPath;
import com.jivesoftware.os.tasmo.model.path.ModelPathStep;
import com.jivesoftware.os.tasmo.model.path.ModelPathStepType;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import org.testng.Assert;
import org.testng.annotations.Test;
import org.testng.collections.Lists;

/**
 *
 */
public class ViewFieldCollectorTest {

    @Test
    public void testLatestBackRefOutOfOrderColumns() throws Exception {

        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(SerializationFeature.INDENT_OUTPUT, true);

        JsonViewMerger merger = new JsonViewMerger(mapper);

        ModelPath path = new ModelPath("path", Arrays.asList(
                new ModelPathStep(false, Sets.newHashSet("DocumentVersion"), "parent", ModelPathStepType.latest_backRef,
                Sets.newHashSet("Document"), null),
                new ModelPathStep(true, Sets.newHashSet("DocumentVersion"), null, ModelPathStepType.value,
                null, Lists.newArrayList("body", "subject"))));

        ViewFieldCollector viewFieldCollector = new ViewFieldCollector(merger, path,
                new String[]{"Document", "DocumentVersion"});

        ObjectId[] pathIds = new ObjectId[]{new ObjectId("Document", new Id(34)), new ObjectId("DocumentVersion", new Id(56))};
        viewFieldCollector.collect(pathIds, "{\"subject\":\"first subject\", \"body\":\"first body\"}", 1);

        Set<Id> ids = new HashSet<>();
        ids.add(pathIds[0].getId());
        ids.add(pathIds[1].getId());

        pathIds = new ObjectId[]{new ObjectId("Document", new Id(34)), new ObjectId("DocumentVersion", new Id(57))};
        viewFieldCollector.collect(pathIds, "{\"subject\":\"third subject\", \"body\":\"third body\"}", 3);

        ids.add(pathIds[0].getId());
        ids.add(pathIds[1].getId());

        pathIds = new ObjectId[]{new ObjectId("Document", new Id(34)), new ObjectId("DocumentVersion", new Id(58))};
        viewFieldCollector.collect(pathIds, "{\"subject\":\"second subject\", \"body\":\"second body\"}", 2);

        ids.add(pathIds[0].getId());
        ids.add(pathIds[1].getId());

        ObjectNode result = merger.createObjectNode();

        merger.merge(result, viewFieldCollector.result(ids));

        System.out.println(mapper.writeValueAsString(result));

        Assert.assertNotNull(result.get("latest_parent"));
        Assert.assertTrue(result.get("latest_parent").isObject());

        ObjectNode child = (ObjectNode) result.get("latest_parent");
        Assert.assertNotNull(child.get("subject"));
        Assert.assertEquals(child.get("subject").textValue(), "third subject");

    }

    @Test
    public void testBranching() throws Exception {

        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(SerializationFeature.INDENT_OUTPUT, true);

        JsonViewMerger merger = new JsonViewMerger(mapper);

        ModelPath path = ModelPath.builder("PathId").
                addPathMember(new ModelPathStep(true, Sets.newHashSet("A"), "fieldName", ModelPathStepType.refs, Sets.newHashSet("B"), null)).
                addPathMember(new ModelPathStep(false, Sets.newHashSet("B"), "fieldName", ModelPathStepType.refs, Sets.newHashSet("C"), null)).
                addPathMember(new ModelPathStep(false, Sets.newHashSet("C"), "fieldName", ModelPathStepType.ref, Sets.newHashSet("D"), null)).
                addPathMember(new ModelPathStep(false, Sets.newHashSet("D"), null, ModelPathStepType.value, null, Arrays.asList("dField"))).build();

        ViewFieldCollector viewFieldCollector = new ViewFieldCollector(merger, path, new String[]{"A", "B", "C", "D"});


        ObjectId[] pathIds = new ObjectId[]{new ObjectId("A", new Id(1)), new ObjectId("B", new Id(2)), new ObjectId("C", new Id(3)),
            new ObjectId("D", new Id(4))};
        viewFieldCollector.collect(pathIds, "{\"dField\":\"first\"}", 4);

        pathIds = new ObjectId[]{new ObjectId("A", new Id(1)), new ObjectId("B", new Id(2)), new ObjectId("C", new Id(5)),
            new ObjectId("D", new Id(6))};
        viewFieldCollector.collect(pathIds, "{\"dField\":\"second\"}", 3);

        pathIds = new ObjectId[]{new ObjectId("A", new Id(1)), new ObjectId("B", new Id(7)), new ObjectId("C", new Id(8)),
            new ObjectId("D", new Id(9))};
        viewFieldCollector.collect(pathIds, "{\"dField\":\"third\"}", 2);

        pathIds = new ObjectId[]{new ObjectId("A", new Id(1)), new ObjectId("B", new Id(7)), new ObjectId("C", new Id(10)),
            new ObjectId("D", new Id(11))};
        viewFieldCollector.collect(pathIds, "{\"dField\":\"fourth\"}", 1);


        Set<Id> validIds = new HashSet<>();
        for (int i = 1; i < 12; i++) {
            validIds.add(new Id(i));
        }

        ObjectNode view = viewFieldCollector.result(validIds);

        System.out.println(mapper.writeValueAsString(view));


    }
}
