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

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.jivesoftware.os.tasmo.id.Id;
import com.jivesoftware.os.tasmo.id.ObjectId;
import com.jivesoftware.os.tasmo.model.path.ModelPath;
import com.jivesoftware.os.tasmo.model.path.ModelPathStep;
import com.jivesoftware.os.tasmo.model.path.ModelPathStepType;
import com.jivesoftware.os.tasmo.view.reader.api.ViewDescriptor;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 *
 */
public class ViewAccumulator<V> {

    private final ViewPermissionChecker viewPermissionChecker;
    private final ExistenceChecker existenceChecker;
    private final Map<ViewDescriptor, AccumulationContext> accumulatingViews = new HashMap<>();

    public ViewAccumulator(Map<ViewDescriptor, RootAndPaths> requestsAndPaths,
        ViewPermissionChecker viewPermissionChecker, ExistenceChecker existenceChecker) {
        for (Map.Entry<ViewDescriptor, RootAndPaths> entry : requestsAndPaths.entrySet()) {
            accumulatingViews.put(entry.getKey(), new AccumulationContext(entry.getValue()));
        }
        this.viewPermissionChecker = viewPermissionChecker;
        this.existenceChecker = existenceChecker;
    }

    public void addRefResults(Map<ViewDescriptor, Multimap<String, ViewReference>> referenceSteps) {
        for (Map.Entry<ViewDescriptor, Multimap<String, ViewReference>> entry : referenceSteps.entrySet()) {

            AccumulationContext context = accumulatingViews.get(entry.getKey());
            Multimap<String, ViewReference> viewReferenceSteps = entry.getValue();
            for (String pathId : viewReferenceSteps.keySet()) {
                Collection<ViewReference> toAdd = viewReferenceSteps.get(pathId);

                for (ViewReference reference : toAdd) {
                    context.presentIds.addAll(reference.getDestinationIds());
                }
            }

            context.refResults.add(viewReferenceSteps);
        }
    }

    public boolean forbidden(ViewDescriptor viewDescriptor) {
        return accumulatingViews.get(viewDescriptor).forbidden;
    }

    public V formatResults(ViewDescriptor viewDescriptor, ViewFormatter<V> formatter) {
        final AccumulationContext context = accumulatingViews.get(viewDescriptor);
        context.presentIds.retainAll(existenceChecker.check(viewDescriptor.getTenantId(), context.presentIds));

        //visibility only using id is awkward
        Set<Id> visibleIds = viewPermissionChecker.check(viewDescriptor.getTenantId(), viewDescriptor.getActorId(),
            Sets.newHashSet(Iterables.transform(context.presentIds, new Function<ObjectId, Id>() {
            @Override
            public Id apply(ObjectId f) {
                return f.getId();
            }
        }))).allowed();

        Set<ObjectId> toRemove = new HashSet<>();
        for (ObjectId objectId : context.presentIds) {
            if (!visibleIds.contains(objectId.getId())) {
                toRemove.add(objectId);
            }
        }
        context.presentIds.removeAll(toRemove);
        context.forbidden = toRemove.contains(context.viewRoot);

        if (context.presentIds.contains(context.viewRoot)) {

            formatter.setRoot(context.viewRoot);

            for (ModelPath path : context.paths) {
                for (Multimap<String, ViewReference> treeLevel : context.refResults) {
                    for (ViewReference reference : treeLevel.get(path.getId())) {
                        if (context.presentIds.contains(reference.getOriginId())) {
                            List<ObjectId> presentDestinations = Lists.newArrayList(Iterables.filter(reference.getDestinationIds(),
                                new Predicate<ObjectId>() {
                                @Override
                                public boolean apply(ObjectId t) {
                                    return context.presentIds.contains(t);
                                }
                            }));
                            formatter.addReferenceNode(reference, presentDestinations);
                        }
                    }

                    formatter.nextLevel();
                }

                for (ViewValue value : context.valueResults.get(path.getId())) {
                    if (context.presentIds.contains(value.getObjectId())) {
                        formatter.addValueNode(value);
                    }
                }

                formatter.nextPath();
            }

            return formatter.getView();
        } else {
            return null;
        }
    }

    public Map<ViewDescriptor, Multimap<String, ViewReference>> buildNextViewLevel() {
        Map<ViewDescriptor, Multimap<String, ViewReference>> nextLevel = new HashMap<>();
        for (Map.Entry<ViewDescriptor, AccumulationContext> entry : accumulatingViews.entrySet()) {
            Multimap<String, ViewReference> nextLevelForView = buildNextLevelForView(entry.getKey());
            if (!nextLevelForView.isEmpty()) {
                nextLevel.put(entry.getKey(), nextLevelForView);
            }
        }

        return nextLevel;
    }

    private Multimap<String, ViewReference> buildNextLevelForView(ViewDescriptor viewDescriptor) {
        AccumulationContext context = accumulatingViews.get(viewDescriptor);
        List<Multimap<String, ViewReference>> refResults = context.refResults;
        int nextLevelIdx = refResults.size();

        Multimap<String, ViewReference> nextLevel = ArrayListMultimap.create();

        if (nextLevelIdx > 0) {
            Multimap<String, ViewReference> lastLevel = refResults.get(nextLevelIdx - 1);

            for (String pathId : lastLevel.keySet()) {
                for (ViewReference reference : lastLevel.get(pathId)) {
                    ModelPath path = reference.getPath();
                    ModelPathStep nextStep = path.getPathMembers().get(nextLevelIdx);

                    if (ModelPathStepType.value.equals(nextStep.getStepType())) {
                        for (ObjectId source : reference.getDestinationIds()) {
                            context.valueResults.put(pathId, new ViewValue(reference.getPath(), nextStep, source));
                        }
                    } else {
                        for (ObjectId source : reference.getDestinationIds()) {
                            nextLevel.put(pathId, new ViewReference(reference.getPath(), nextStep, source));
                        }
                    }

                }
            }
        } else {
            for (ModelPath path : context.paths) {
                ModelPathStep step = path.getPathMembers().get(0);
                if (ModelPathStepType.value.equals(step.getStepType())) {
                    context.valueResults.put(path.getId(), new ViewValue(path, step, context.viewRoot));

                } else {
                    nextLevel.put(path.getId(), new ViewReference(path, step, context.viewRoot));
                }
            }
        }

        return nextLevel;
    }

    public Map<ViewDescriptor, Multimap<String, ViewValue>> getViewValues(Iterable<ViewDescriptor> viewDescriptors) {
        Map<ViewDescriptor, Multimap<String, ViewValue>> allViewValues = new HashMap<>();
        for (ViewDescriptor descriptor : viewDescriptors) {
            Multimap<String, ViewValue> viewValues = accumulatingViews.get(descriptor).valueResults;
            if (viewValues != null) {
                allViewValues.put(descriptor, viewValues);
            }
        }

        return allViewValues;
    }

    static class RootAndPaths {

        final ObjectId viewRoot;
        final List<ModelPath> paths;

        RootAndPaths(ObjectId viewRoot, List<ModelPath> paths) {
            this.viewRoot = viewRoot;
            this.paths = paths;
        }
    }

    //holds accummulated state for a single view instance
    private static class AccumulationContext {

        private final ObjectId viewRoot;
        private final List<ModelPath> paths;
        private final Set<ObjectId> presentIds = new HashSet<>();
        private boolean forbidden;
        private final List<Multimap<String, ViewReference>> refResults = new ArrayList<>();
        private final Multimap<String, ViewValue> valueResults = ArrayListMultimap.create();

        public AccumulationContext(RootAndPaths rootAndPaths) {
            this.viewRoot = rootAndPaths.viewRoot;
            this.paths = rootAndPaths.paths;
            this.presentIds.add(viewRoot);
        }
    }
}
