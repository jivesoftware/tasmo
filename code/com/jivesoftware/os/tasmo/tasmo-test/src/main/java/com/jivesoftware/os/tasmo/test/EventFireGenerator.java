/*
 * $Revision$
 * $Date$
 *
 * Copyright (C) 1999-$year$ Jive Software. All rights reserved.
 *
 * This software is the proprietary information of Jive Software. Use is subject to license terms.
 */
package com.jivesoftware.os.tasmo.test;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.jivesoftware.os.jive.utils.id.Id;
import com.jivesoftware.os.jive.utils.id.IdProvider;
import com.jivesoftware.os.jive.utils.id.ObjectId;
import com.jivesoftware.os.jive.utils.id.TenantId;
import com.jivesoftware.os.tasmo.event.api.write.Event;
import com.jivesoftware.os.tasmo.event.api.write.EventBuilder;
import com.jivesoftware.os.tasmo.model.path.ModelPath;
import com.jivesoftware.os.tasmo.model.path.ModelPathStep;
import com.jivesoftware.os.tasmo.model.path.ModelPathStepType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Generates test input for tasmo tests
 *
 */
public class EventFireGenerator {

    private final TenantId tenant;
    private final Id actor;

    public EventFireGenerator(TenantId tenant, Id actor) {
        this.tenant = tenant;
        this.actor = actor;
    }

    public static void main(String[] args) {
        IdProvider idProvider = new IdProvider() {
            private long number;

            @Override
            public Id nextId() {
                ++number;
                return new Id(number);
            }
        };
        EventFireGenerator eventFireGenerator = new EventFireGenerator(new TenantId("test"), new Id("actor"));

        List<ModelPathStep> steps = new ArrayList<>();
        steps.add(new ModelPathStep(true, Sets.newHashSet("A"), "ref_B", ModelPathStepType.ref, Sets.newHashSet("B"), null));
        steps.add(new ModelPathStep(false, Sets.newHashSet("C"), "backrefs_B", ModelPathStepType.backRefs, Sets.newHashSet("B"), null));
        steps.add(new ModelPathStep(false, Sets.newHashSet("C"), "refs_D", ModelPathStepType.refs, Sets.newHashSet("D"), null));
        steps.add(new ModelPathStep(false, Sets.newHashSet("D"), null, ModelPathStepType.value, null, Arrays.asList("Value1", "Value2", "Value3")));

        ModelPath path = new ModelPath("testpath", steps);

        EventsAndViewId eventsAndViewId = eventFireGenerator.deriveEventsFromPath(idProvider, path, idProvider.nextId(), 2);

        for (EventFire eventFire : eventFireGenerator.generateEventFireCombinationsForPath("TestView", eventsAndViewId, path)) {
            StringBuilder builder = new StringBuilder("Event fire: ");
            String sep = "";
            for (Event event : eventFire.getFiredEvents()) {
                builder.append(sep).append(event);
                sep = ", ";
            }

            System.out.println(builder.toString());
        }
    }

    public List<EventFire> generateEventFireCombinationsForPath(String viewClass, EventsAndViewId eventsAndViewId, ModelPath path) {
        List<EventFire> eventFires = new ArrayList<>();
        List<Event> events = eventsAndViewId.getEvents();
        ObjectId viewId = new ObjectId(viewClass, eventsAndViewId.getViewId());

        for (int i = 0; i < events.size(); i++) {
            Collections.rotate(events, 1);
            eventFires.add(new EventFire(viewId, events, path.getPathMembers().get(path.getPathMemberSize() - 1), eventsAndViewId.getIdTree()));
        }

        Collections.reverse(events);

        for (int i = 0; i < events.size(); i++) {
            Collections.rotate(events, 1);
            eventFires.add(new EventFire(viewId, events, path.getPathMembers().get(path.getPathMemberSize() - 1), eventsAndViewId.getIdTree()));
        }

        return eventFires;
    }

    public EventsAndViewId deriveEventsFromPath(IdProvider idProvider, ModelPath path, Id viewId, int fanOut) {
        List<Event> events = new ArrayList<>();

        IdTreeNode rootId = new IdTreeNode(null, viewId);

        generateIdsForPathStep(idProvider, rootId, 0, path, fanOut);

        generateEventsForPathStep(rootId, 0, path, events);

        return new EventsAndViewId(viewId, events, rootId);
    }

    private void generateIdsForPathStep(IdProvider idProvider, IdTreeNode previousId, int pathIndex, ModelPath path, int fanOut) {
        if (pathIndex < path.getPathMemberSize()) {
            ModelPathStep currentStep = path.getPathMembers().get(pathIndex);
            ModelPathStepType currentType = currentStep.getStepType();

            switch (currentType) {
                case ref:
                    IdTreeNode referencedId = new IdTreeNode(previousId, idProvider.nextId());
                    generateIdsForPathStep(idProvider, referencedId, pathIndex + 1, path, fanOut);
                    break;
                case refs:
                case backRefs:
                case count:
                    for (int i = 0; i < fanOut; i++) {
                        IdTreeNode idTreeNode = new IdTreeNode(previousId, idProvider.nextId());
                        generateIdsForPathStep(idProvider, idTreeNode, pathIndex + 1, path, fanOut);
                    }
                    break;
                case latest_backRef:
                    IdTreeNode referencingId = new IdTreeNode(previousId, idProvider.nextId());
                    generateIdsForPathStep(idProvider, referencingId, pathIndex + 1, path, fanOut);
                    break;
                case value:
                    return; //id for this node was generated in the previous step
                default:
                    throw new IllegalArgumentException("Unhandled model path step type: " + currentType);
            }
        }
    }

    private void generateEventsForPathStep(IdTreeNode currentId, int pathIndex, ModelPath path, List<Event> accumulator) {
        if (pathIndex < path.getPathMemberSize()) {
            ModelPathStep currentStep = path.getPathMembers().get(pathIndex);
            ModelPathStepType currentType = currentStep.getStepType();
            List<IdTreeNode> nextIds = currentId.children();
            String currentOrigin = currentStep.getOriginClassNames().iterator().next();
            String currentDestination = currentType.equals(ModelPathStepType.value) ? null : currentStep.getDestinationClassNames().iterator().next();

            switch (currentType) {
                case ref:

                    accumulator.add(generateEvent(new ObjectId(currentOrigin, currentId.value()), currentStep,
                            Arrays.asList(new ObjectId(currentDestination, nextIds.get(0).value()))));
                    break;
                case refs:

                    ObjectId referrer = new ObjectId(currentOrigin, currentId.value());
                    List<ObjectId> referenced = new ArrayList<>();
                    for (IdTreeNode nextNode : nextIds) {
                        referenced.add(new ObjectId(currentDestination, nextNode.value()));
                    }
                    accumulator.add(generateEvent(referrer, currentStep, referenced));
                    break;
                case backRefs:
                case count:
                    ObjectId referred = new ObjectId(currentDestination, currentId.value());
                    for (IdTreeNode nextNode : nextIds) {
                        accumulator.add(generateEvent(new ObjectId(currentOrigin, nextNode.value()), currentStep, Arrays.asList(referred)));
                    }
                    accumulator.add(generateEvent(referred, currentStep, null));
                    break;
                case latest_backRef:

                    accumulator.add(generateEvent(new ObjectId(currentOrigin, nextIds.get(0).value()), currentStep,
                            Arrays.asList(new ObjectId(currentDestination, currentId.value()))));
                    accumulator.add(generateEvent(new ObjectId(currentDestination, currentId.value()), currentStep, null));
                    break;
                case value:
                    accumulator.add(generateEvent(new ObjectId(currentOrigin, currentId.value()), currentStep, null));
                    break;
                default:
                    throw new IllegalArgumentException("Unhandled model path step type: " + currentType);
            }

            for (IdTreeNode next : nextIds) {
                generateEventsForPathStep(next, pathIndex + 1, path, accumulator);
            }
        }
    }

    private Event generateEvent(ObjectId eventObjectId, ModelPathStep currentStep, List<ObjectId> referencedIds) {
        EventBuilder builder = EventBuilder.update(eventObjectId, tenant, actor);
        ModelPathStepType currentType = currentStep.getStepType();
        if (ModelPathStepType.value.equals(currentType)) {
            for (String fieldName : currentStep.getFieldNames()) {
                builder.set(fieldName, "ValueFor_" + fieldName);
            }
        } else if (ModelPathStepType.refs.equals(currentType) || ModelPathStepType.backRefs.equals(currentType)) {
            if (referencedIds != null) {
                builder.set(currentStep.getRefFieldName(), Lists.transform(referencedIds, new Function<ObjectId, String>() {
                    @Override
                    public String apply(ObjectId referencedId) {
                        return referencedId.toStringForm();
                    }
                }));
            } else {
                builder.set("deleted", false);
            }
        } else {
            if (referencedIds != null) {
                builder.set(currentStep.getRefFieldName(), referencedIds.get(0).toStringForm());
            } else {
                builder.set("deleted", false);
            }
        }

        return builder.build();

    }
}