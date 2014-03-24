/*
 * $Revision$
 * $Date$
 *
 * Copyright (C) 1999-$year$ Jive Software. All rights reserved.
 *
 * This software is the proprietary information of Jive Software. Use is subject to license terms.
 */
package com.jivesoftware.os.tasmo.configuration;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Sets;
import com.jivesoftware.os.tasmo.model.EventDefinition;
import com.jivesoftware.os.tasmo.model.EventsModel;
import com.jivesoftware.os.tasmo.model.ViewBinding;
import com.jivesoftware.os.tasmo.model.path.ModelPath;
import com.jivesoftware.os.tasmo.model.path.ModelPathStep;
import com.jivesoftware.os.tasmo.model.path.ModelPathStepType;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 *
 */
public class BindingGeneratorTest {

    private final ObjectMapper mapper = new ObjectMapper();
    private final Set<String> content = Sets.newHashSet("Content");
    private final Set<String> tag = Sets.newHashSet("Tag");
    private final Set<String> container = Sets.newHashSet("Container");
    private final Set<String> user = Sets.newHashSet("User");

    @Test
    public void testBindingGeneration() throws Exception {

        ModelPath a = ModelPath.builder("Content.value").
            addPathMember(new ModelPathStep(true, content, null, ModelPathStepType.value, null, Arrays.asList("title"))).build();
        ModelPath b = ModelPath.builder("Content.parent.ref.Container.tags.refs.Tag.value").
            addPathMember(new ModelPathStep(true, content, "parent", ModelPathStepType.ref, container, null)).
            addPathMember(new ModelPathStep(false, container, "tags", ModelPathStepType.refs, tag, null)).
            addPathMember(new ModelPathStep(false, tag, null, ModelPathStepType.value, null, Arrays.asList("name"))).build();

        mapper.configure(SerializationFeature.INDENT_OUTPUT, true);

        EventsModel eventsModel = new EventsModel();
        eventsModel.addEvent(toEvent("Container", new Container()));
        eventsModel.addEvent(toEvent("Content", new Content()));
        eventsModel.addEvent(toEvent("User", new User()));
        eventsModel.addEvent(toEvent("Tag", new Tag()));

        BindingGenerator bindingGenerator = new BindingGenerator();

        ViewModel viewConfiguration = toView("View", new ContentView());
        ViewBinding generate = bindingGenerator.generate(eventsModel, viewConfiguration);

        Set<ModelPath> found = new HashSet<>(generate.getModelPaths());

        Assert.assertTrue(found.contains(a));
        Assert.assertTrue(found.contains(b));

    }

    @Test
    public void testBackRefBindingGeneration() throws Exception {

        ModelPath a = ModelPath.builder("Container.value").
            addPathMember(new ModelPathStep(true, container, null, ModelPathStepType.value, null, Arrays.asList("name"))).build();
//        ModelPath b = ModelPath.builder("Container.parent.backRefs.Content.authors.refs.User.value").
//                addPathMember(new ModelPathStep(true, content, "parent", ModelPathStepType.backRefs, container, null)).
//                addPathMember(new ModelPathStep(false, content, "authors", ModelPathStepType.refs, user, null)).
//                addPathMember(new ModelPathStep(false, user, null, ModelPathStepType.value, null, Arrays.asList("lastName", "firstName"))).build();

        mapper.configure(SerializationFeature.INDENT_OUTPUT, true);

        EventsModel eventsModel = new EventsModel();
        eventsModel.addEvent(toEvent("Container", new Container()));
        eventsModel.addEvent(toEvent("Content", new Content()));
        eventsModel.addEvent(toEvent("User", new User()));

        BindingGenerator bindingGenerator = new BindingGenerator();

        ViewModel viewConfiguration = toView("View", new ContainerView());
        ViewBinding generated = bindingGenerator.generate(eventsModel, viewConfiguration);

        Set<ModelPath> found = new HashSet<>(generated.getModelPaths());

        Assert.assertTrue(found.contains(a));
//        Assert.assertTrue(found.contains(b));

    }

    @Test
    public void testLoooongPath() throws Exception {

        ModelPath a = ModelPath.builder("User.value").
                addPathMember(new ModelPathStep(true, user, null, ModelPathStepType.value, null, Arrays.asList("firstName"))).build();

        ModelPath b = ModelPath.builder("User.authors.backRefs.Content.value").
            addPathMember(new ModelPathStep(true, content, "authors", ModelPathStepType.backRefs, user, null)).
            addPathMember(new ModelPathStep(false, content, null, ModelPathStepType.value, null, Arrays.asList("title"))).build();

        ModelPath c = ModelPath.builder("User.authors.backRefs.Content.parent.ref.Container.parent.ref.Container.value").
                addPathMember(new ModelPathStep(true, content, "authors", ModelPathStepType.backRefs, user, null)).
                addPathMember(new ModelPathStep(false, content, "parent", ModelPathStepType.ref, container, null)).
                addPathMember(new ModelPathStep(false, container, "parent", ModelPathStepType.ref, container, null)).
                addPathMember(new ModelPathStep(false, container, null, ModelPathStepType.value, null, Arrays.asList("name"))).build();

        ModelPath d = ModelPath.builder("User.authors.backRefs.Content.parent.ref.Container.parent.ref.Container.tags.refs.Tag.value").
                addPathMember(new ModelPathStep(true, content, "authors", ModelPathStepType.backRefs, user, null)).
                addPathMember(new ModelPathStep(false, content, "parent", ModelPathStepType.ref, container, null)).
                addPathMember(new ModelPathStep(false, container, "parent", ModelPathStepType.ref, container, null)).
                addPathMember(new ModelPathStep(false, container, "tags", ModelPathStepType.refs, tag, null)).
                addPathMember(new ModelPathStep(false, tag, null, ModelPathStepType.value, null, Arrays.asList("name"))).build();

        ModelPath e = ModelPath.builder("User.authors.backRefs.Content.parent.ref.Container.parent.ref.Container.tags.refs.Tag.author.ref.User.value").
                addPathMember(new ModelPathStep(true, content, "authors", ModelPathStepType.backRefs, user, null)).
                addPathMember(new ModelPathStep(false, content, "parent", ModelPathStepType.ref, container, null)).
                addPathMember(new ModelPathStep(false, container, "parent", ModelPathStepType.ref, container, null)).
                addPathMember(new ModelPathStep(false, container, "tags", ModelPathStepType.refs, tag, null)).
                addPathMember(new ModelPathStep(false, tag, "author", ModelPathStepType.ref, user, null)).
                addPathMember(new ModelPathStep(false, user, null, ModelPathStepType.value, null, Arrays.asList("lastName"))).build();

        mapper.configure(SerializationFeature.INDENT_OUTPUT, true);

        EventsModel eventsModel = new EventsModel();
        eventsModel.addEvent(toEvent("Container", new Container()));
        eventsModel.addEvent(toEvent("Content", new Content()));
        eventsModel.addEvent(toEvent("User", new User()));
        eventsModel.addEvent(toEvent("Tag", new Tag()));

        BindingGenerator bindingGenerator = new BindingGenerator();

        ViewModel viewConfiguration = toView("View", new UserView());
        ViewBinding generated = bindingGenerator.generate(eventsModel, viewConfiguration);

        Set<ModelPath> found = new HashSet<>(generated.getModelPaths());

        Assert.assertTrue(found.contains(a));
        Assert.assertTrue(found.contains(b));
        Assert.assertTrue(found.contains(c));
        Assert.assertTrue(found.contains(d));
        Assert.assertTrue(found.contains(e));

    }

    private EventDefinition toEvent(String label, Object instance) throws IOException {
        ObjectNode event = toObjectNode(label, instance);
        return EventDefinition.builder(event, true).build();
    }

    private ViewModel toView(String viewclass, Object instance) throws IOException {
        ObjectNode view = toObjectNode(viewclass, instance);
        return ViewModel.builder(view).build();
    }

    private ObjectNode toObjectNode(String label, Object instance) throws IOException {
        ObjectNode convertValue = mapper.convertValue(instance, ObjectNode.class);

        ObjectNode node = mapper.createObjectNode();
        node.put(label, convertValue);
        System.out.println(label);
        System.out.println(mapper.writeValueAsString(node));
        System.out.println();

        return node;
    }
}
