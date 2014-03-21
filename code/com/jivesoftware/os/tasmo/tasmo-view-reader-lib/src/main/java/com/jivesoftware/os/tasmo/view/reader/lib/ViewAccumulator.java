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
import com.jivesoftware.os.tasmo.id.TenantId;
import com.jivesoftware.os.tasmo.model.path.ModelPath;
import com.jivesoftware.os.tasmo.model.path.ModelPathStep;
import com.jivesoftware.os.tasmo.model.path.ModelPathStepType;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 *
 */
public class ViewAccumulator<V> {

    private final ViewPermissionChecker viewPermissionChecker;
    private final ExistenceChecker existenceChecker;
    private List<Multimap<String, ViewReference>> refResults = new ArrayList<>();
    private Multimap<String, ViewValue> valueResults = ArrayListMultimap.create();
    private Set<ObjectId> presentIds = new HashSet<>();
    private final List<ModelPath> allPaths;
    private final ObjectId viewRoot;
    private boolean forbidden;

    public ViewAccumulator(ObjectId viewRoot, List<ModelPath> allPaths,
        ViewPermissionChecker viewPermissionChecker, ExistenceChecker existenceChecker) {
        this.viewRoot = viewRoot;
        this.allPaths = allPaths;
        this.viewPermissionChecker = viewPermissionChecker;
        this.existenceChecker = existenceChecker;
        this.presentIds.add(viewRoot);
    }

    public void addRefResults(Multimap<String, ViewReference> referenceSteps) {
        for (String pathId : referenceSteps.keySet()) {
            Collection<ViewReference> toAdd = referenceSteps.get(pathId);

            for (ViewReference reference : toAdd) {
                presentIds.addAll(reference.getDestinationIds());
            }
        }

        refResults.add(referenceSteps);
    }
    
    public boolean forbidden() {
        return forbidden;
    }

    public V formatResults(TenantId tenantId, Id actorId, ViewFormatter<V> formatter) {
        presentIds.retainAll(existenceChecker.check(tenantId, presentIds));
       
        //visibility only using id is awkward
        Set<Id> visibleIds = viewPermissionChecker.check(tenantId, actorId,
            Sets.newHashSet(Iterables.transform(presentIds, new Function<ObjectId, Id>(){

            @Override
            public Id apply(ObjectId f) {
                return f.getId();
            }
            
        }))).allowed();
        
        Set<ObjectId> toRemove = new HashSet<>();
        for (ObjectId objectId : presentIds) {
            if (!visibleIds.contains(objectId.getId())) {
                toRemove.add(objectId);
            }
        }
        presentIds.removeAll(toRemove);
        forbidden = toRemove.contains(viewRoot);
        
        if (presentIds.contains(viewRoot)) {
            
            formatter.setRoot(viewRoot);

            for (ModelPath path : allPaths) {
                for (Multimap<String, ViewReference> treeLevel : refResults) {
                    for (ViewReference reference : treeLevel.get(path.getId())) {
                        if (presentIds.contains(reference.getOriginId())) {
                            List<ObjectId> presentDestinations = Lists.newArrayList(Iterables.filter(reference.getDestinationIds(),
                                new Predicate<ObjectId>(){

                                @Override
                                public boolean apply(ObjectId t) {
                                    return presentIds.contains(t);
                                }
                                
                            }));
                            formatter.addReferenceNode(reference, presentDestinations);
                        }
                    }
                    
                    formatter.nextLevel();
                }

                for (ViewValue value : valueResults.get(path.getId())) {
                    if (presentIds.contains(value.getObjectId())) {
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

    public Multimap<String, ViewReference> buildNextViewLevel() {
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
                            valueResults.put(pathId, new ViewValue(reference.getPath(), nextStep, source));
                        }
                    } else {
                        for (ObjectId source : reference.getDestinationIds()) {
                            nextLevel.put(pathId, new ViewReference(reference.getPath(), nextStep, source));
                        }
                    }

                }
            }
        } else {
            for (ModelPath path : allPaths) {
                ModelPathStep step = path.getPathMembers().get(0);
                if (ModelPathStepType.value.equals(step.getStepType())) {
                    valueResults.put(path.getId(), new ViewValue(path, step, viewRoot));

                } else {
                    nextLevel.put(path.getId(), new ViewReference(path, step, viewRoot));
                }
            }
        }

        return nextLevel;
    }

    public Multimap<String, ViewValue> getViewValues() {
        return valueResults;
    }
}
