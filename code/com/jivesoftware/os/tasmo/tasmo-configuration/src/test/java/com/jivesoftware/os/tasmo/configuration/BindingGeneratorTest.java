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
                addPathMember(new ModelPathStep(true, content, null, ModelPathStepType.value, null, Arrays.asList("title"), false)).build();
        ModelPath b = ModelPath.builder("Content.parent.ref.Container.tags.refs.Tag.value").
                addPathMember(new ModelPathStep(true, content, "parent", ModelPathStepType.ref, container, null, false)).
                addPathMember(new ModelPathStep(false, container, "tags", ModelPathStepType.refs, tag, null, false)).
                addPathMember(new ModelPathStep(false, tag, null, ModelPathStepType.value, null, Arrays.asList("name"), false)).build();

        mapper.configure(SerializationFeature.INDENT_OUTPUT, true);

        EventsModel eventsModel = new EventsModel();
        eventsModel.addEvent(toObject("Event", new Container()));
        eventsModel.addEvent(toObject("Event", new Content()));
        eventsModel.addEvent(toObject("Event", new User()));
        eventsModel.addEvent(toObject("Event", new Tag()));

        BindingGenerator bindingGenerator = new BindingGenerator();

        ObjectNode contentView = toObject("View", new ContentView());
        ViewModel viewConfiguration = ViewModel.builder(contentView).build();
        ViewBinding generate = bindingGenerator.generate(eventsModel, viewConfiguration);

        Set<ModelPath> found = new HashSet<>(generate.getModelPaths());

        Assert.assertTrue(found.contains(a));
        Assert.assertTrue(found.contains(b));

    }

    @Test
    public void testBackRefBindingGeneration() throws Exception {

        ModelPath a = ModelPath.builder("Container.value").
                addPathMember(new ModelPathStep(true, container, null, ModelPathStepType.value, null, Arrays.asList("name"), false)).build();
//        ModelPath b = ModelPath.builder("Container.parent.backrefs.Content.authors.refs.User.value").
//                addPathMember(new ModelPathStep(true, content, "parent", ModelPathStepType.backRefs, container, null)).
//                addPathMember(new ModelPathStep(false, content, "authors", ModelPathStepType.refs, user, null)).
//                addPathMember(new ModelPathStep(false, user, null, ModelPathStepType.value, null, Arrays.asList("lastName", "firstName"))).build();

        mapper.configure(SerializationFeature.INDENT_OUTPUT, true);

        EventsModel eventsModel = new EventsModel();
        eventsModel.addEvent(toObject("Event", new Container()));
        eventsModel.addEvent(toObject("Event", new Content()));
        eventsModel.addEvent(toObject("Event", new User()));

        BindingGenerator bindingGenerator = new BindingGenerator();

        ObjectNode containerView = toObject("View", new ContainerView());
        ViewModel viewConfiguration = ViewModel.builder(containerView).build();
        ViewBinding generated = bindingGenerator.generate(eventsModel, viewConfiguration);

        Set<ModelPath> found = new HashSet<>(generated.getModelPaths());

        Assert.assertTrue(found.contains(a));
//        Assert.assertTrue(found.contains(b));

    }

    @Test
    public void testLoooongPath() throws Exception {

        ModelPath a = ModelPath.builder("User.value").
                addPathMember(new ModelPathStep(true, user, null, ModelPathStepType.value, null, Arrays.asList("firstName"), false)).build();

        ModelPath b = ModelPath.builder("User.authors.backrefs.Content.value").
                addPathMember(new ModelPathStep(true, content, "authors", ModelPathStepType.backRefs, user, null, false)).
                addPathMember(new ModelPathStep(false, content, null, ModelPathStepType.value, null, Arrays.asList("title"), false)).build();

        ModelPath c = ModelPath.builder("User.authors.backrefs.Content.parent.ref.Container.parent.ref.Container.value").
                addPathMember(new ModelPathStep(true, content, "authors", ModelPathStepType.backRefs, user, null, false)).
                addPathMember(new ModelPathStep(false, content, "parent", ModelPathStepType.ref, container, null, false)).
                addPathMember(new ModelPathStep(false, container, "parent", ModelPathStepType.ref, container, null, false)).
                addPathMember(new ModelPathStep(false, container, null, ModelPathStepType.value, null, Arrays.asList("name"), false)).build();

        ModelPath d = ModelPath.builder("User.authors.backrefs.Content.parent.ref.Container.parent.ref.Container.tags.refs.Tag.value").
                addPathMember(new ModelPathStep(true, content, "authors", ModelPathStepType.backRefs, user, null, false)).
                addPathMember(new ModelPathStep(false, content, "parent", ModelPathStepType.ref, container, null, false)).
                addPathMember(new ModelPathStep(false, container, "parent", ModelPathStepType.ref, container, null, false)).
                addPathMember(new ModelPathStep(false, container, "tags", ModelPathStepType.refs, tag, null, false)).
                addPathMember(new ModelPathStep(false, tag, null, ModelPathStepType.value, null, Arrays.asList("name"), false)).build();

        ModelPath e = ModelPath.builder("User.authors.backrefs.Content.parent.ref.Container.parent.ref.Container.tags.refs.Tag.author.ref.User.value").
                addPathMember(new ModelPathStep(true, content, "authors", ModelPathStepType.backRefs, user, null, false)).
                addPathMember(new ModelPathStep(false, content, "parent", ModelPathStepType.ref, container, null, false)).
                addPathMember(new ModelPathStep(false, container, "parent", ModelPathStepType.ref, container, null, false)).
                addPathMember(new ModelPathStep(false, container, "tags", ModelPathStepType.refs, tag, null, false)).
                addPathMember(new ModelPathStep(false, tag, "author", ModelPathStepType.ref, user, null, false)).
                addPathMember(new ModelPathStep(false, user, null, ModelPathStepType.value, null, Arrays.asList("lastName"), false)).build();

        mapper.configure(SerializationFeature.INDENT_OUTPUT, true);

        EventsModel eventsModel = new EventsModel();
        eventsModel.addEvent(toObject("Event", new Container()));
        eventsModel.addEvent(toObject("Event", new Content()));
        eventsModel.addEvent(toObject("Event", new User()));
        eventsModel.addEvent(toObject("Event", new Tag()));

        BindingGenerator bindingGenerator = new BindingGenerator();

        ObjectNode containerView = toObject("View", new UserView());
        ViewModel viewConfiguration = ViewModel.builder(containerView).build();
        ViewBinding generated = bindingGenerator.generate(eventsModel, viewConfiguration);

        Set<ModelPath> found = new HashSet<>(generated.getModelPaths());

        Assert.assertTrue(found.contains(a));
        Assert.assertTrue(found.contains(b));
        Assert.assertTrue(found.contains(c));
        Assert.assertTrue(found.contains(d));
        Assert.assertTrue(found.contains(e));

    }

    private ObjectNode toObject(String label, Object instance) throws IOException {
        ObjectNode convertValue = mapper.convertValue(instance, ObjectNode.class);

        ObjectNode event = mapper.createObjectNode();
        event.put(instance.getClass().getSimpleName(), convertValue);
        System.out.println(label);
        System.out.println(mapper.writeValueAsString(event));
        System.out.println();

        return event;
    }
}
