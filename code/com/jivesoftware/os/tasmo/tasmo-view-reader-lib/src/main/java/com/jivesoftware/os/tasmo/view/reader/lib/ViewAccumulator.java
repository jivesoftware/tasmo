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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.jivesoftware.os.tasmo.id.Id;
import com.jivesoftware.os.tasmo.id.ObjectId;
import com.jivesoftware.os.tasmo.id.TenantId;
import com.jivesoftware.os.tasmo.model.process.OpaqueFieldValue;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 *
 */
public class ViewAccumulator {

    private final ObjectMapper mapper;
    private final ViewPermissionChecker viewPermissionChecker;
    private List<Multimap<String, ObjectId>> breadthFirstRefResults = new ArrayList<>();
    private Multimap<String, OpaqueFieldValue>  valueResults = ArrayListMultimap.create();
    private Set<Id> permittedIds = new HashSet<>();

    public ViewAccumulator(ObjectMapper mapper, ViewPermissionChecker viewPermissionChecker) {
        this.mapper = mapper;
        this.viewPermissionChecker = viewPermissionChecker;
    }

    public void addRefResults(Multimap<String, ObjectId> resultsAtTreeLevel) {
        breadthFirstRefResults.add(resultsAtTreeLevel);
        
        for (String pathId : resultsAtTreeLevel.keySet()) {
            for (ObjectId id : resultsAtTreeLevel.get(pathId)) {
                permittedIds.add(id.getId());
            }
        }
    }

    public void addValueResults(Multimap<String, OpaqueFieldValue> valueResults) {
        this.valueResults.putAll(valueResults);
    }

    public ObjectNode formatResults(TenantId tenantId, Id actorId) {
        permittedIds.addAll(viewPermissionChecker.check(tenantId, actorId, permittedIds).allowed());
        return mapper.createObjectNode();
    }

    public void retainAllVisibleIds(TenantId tenantId, Id actorId, ViewPermissionChecker viewPermissionChecker) {
        permittedIds.addAll(viewPermissionChecker.check(tenantId, actorId, permittedIds).allowed());
    }
    
    public Collection<ObjectId> getIdsForPathAndDepth(String modelPathId, int depth) {
        if (depth >= breadthFirstRefResults.size()) {
            return Collections.emptyList();
        } else {
            Multimap<String, ObjectId> resultsAtDepth = breadthFirstRefResults.get(depth);
            Collection<ObjectId> ids = resultsAtDepth.get(modelPathId);
            return ids != null ? ids : Collections.<ObjectId>emptyList();
        }
    }
    
    
}
