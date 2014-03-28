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
package com.jivesoftware.os.tasmo.view.notification.lib;

import com.google.common.collect.Sets;
import com.jivesoftware.os.jive.utils.base.interfaces.CallbackStream;
import com.jivesoftware.os.tasmo.event.api.ReservedFields;
import com.jivesoftware.os.tasmo.id.Id;
import com.jivesoftware.os.tasmo.id.ObjectId;
import com.jivesoftware.os.tasmo.id.TenantId;
import com.jivesoftware.os.tasmo.id.TenantIdAndCentricId;
import com.jivesoftware.os.tasmo.lib.EventWrite;
import com.jivesoftware.os.tasmo.model.ViewBinding;
import com.jivesoftware.os.tasmo.model.path.ModelPath;
import com.jivesoftware.os.tasmo.model.path.ModelPathStep;
import com.jivesoftware.os.tasmo.model.path.ModelPathStepType;
import com.jivesoftware.os.tasmo.model.process.ModifiedViewInfo;
import com.jivesoftware.os.tasmo.model.process.ModifiedViewProvider;
import com.jivesoftware.os.tasmo.model.process.WrittenEvent;
import com.jivesoftware.os.tasmo.model.process.WrittenInstance;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 *
 * @author pete
 */
public class ViewChangeNotifier {

    private final NotifiableViewModelProvider viewsProvider;
    private final ViewRootLocator rootLocator;

    public ViewChangeNotifier(NotifiableViewModelProvider viewsProvider, ViewRootLocator rootLocator) {
        this.viewsProvider = viewsProvider;
        this.rootLocator = rootLocator;
    }

    public void notifyChangedViews(List<EventWrite> eventBatch, ViewChangeNotificationProcessor viewChangeNotificationProcessor) throws Exception {
        for (EventWrite write : eventBatch) {
            ModifiedViewProvider modifiedViewProvider = detectModifiedViews(write);
            if (!modifiedViewProvider.getModifiedViews().isEmpty()) {
                viewChangeNotificationProcessor.process(modifiedViewProvider, write.getWrittenEvent());
            }
        }
    }

    private ModifiedViewProvider detectModifiedViews(EventWrite write) throws Exception {
        final ModifiedViewProvider provider = new ModifiedViewProvider() {
            private final Set<ModifiedViewInfo> modified = new HashSet<>();

            @Override
            public Set<ModifiedViewInfo> getModifiedViews() {
                return modified;
            }

            @Override
            public void add(ModifiedViewInfo viewId) {
                modified.add(viewId);
            }
        };

        for (ViewBinding binding : viewsProvider.getNotifiableBindings(write.getWrittenEvent())) {
            CallbackStream<ModifiedViewInfo> modificationStream = new CallbackStream<ModifiedViewInfo>() {
                @Override
                public ModifiedViewInfo callback(ModifiedViewInfo value) throws Exception {
                    if (value != null) {
                        provider.add(value);
                    }
                    return value;
                }
            };

            detectChangedViewsForBinding(write, binding, modificationStream);
        }

        return provider;
    }

    private void detectChangedViewsForBinding(EventWrite write, ViewBinding binding, CallbackStream<ModifiedViewInfo> modificationStream) throws Exception {
        WrittenEvent event = write.getWrittenEvent();
        WrittenInstance writtenInstance = event.getWrittenInstance();
        ObjectId instanceId = writtenInstance.getInstanceId();
        String eventClass = instanceId.getClassName();
        TenantId tenantId = event.getTenantId();
        Id userId = event.getCentricId();
        TenantIdAndCentricId tenantIdAndCentricId = new TenantIdAndCentricId(tenantId, userId);
        TenantIdAndCentricId globalTenantIdAndCentricId = new TenantIdAndCentricId(tenantId, Id.NULL);

        for (ModelPath path : binding.getModelPaths()) {

            for (int pathIdx = 0; pathIdx < path.getPathMemberSize(); pathIdx++) {
                ModelPathStep step = path.getPathMembers().get(pathIdx);
                Set<String> writtenFields = Sets.newHashSet(writtenInstance.getFieldNames());

                if (step.getOriginClassNames().contains(eventClass) && (!Collections.disjoint(writtenFields, getAllStepFields(step)))) {

                    if (step.getStepType().equals(ModelPathStepType.value)) {

                        rootLocator.locateViewRoots(binding.getViewClassName(), tenantIdAndCentricId, globalTenantIdAndCentricId,
                            instanceId, pathIdx, path, modificationStream);

                    } else if (step.getStepType().equals(ModelPathStepType.ref) || step.getStepType().equals(ModelPathStepType.refs)) {

                        rootLocator.locateViewRoots(binding.getViewClassName(), tenantIdAndCentricId, globalTenantIdAndCentricId,
                            instanceId, pathIdx, path, modificationStream);
                    } else {
                        //all backref types

                        Set<ObjectId> affectedCentricObjects = buildAffectedBackReferencedObjects(write, step.getRefFieldName(),
                            tenantIdAndCentricId.getCentricId());
                        Set<ObjectId> affectedGlobalcObjects = buildAffectedBackReferencedObjects(write, step.getRefFieldName(),
                            globalTenantIdAndCentricId.getCentricId());

                        rootLocator.locateViewRoots(binding.getViewClassName(), tenantIdAndCentricId, globalTenantIdAndCentricId,
                            affectedCentricObjects, affectedGlobalcObjects,
                            pathIdx, path, modificationStream);
                    }
                } else if (step.getStepType().isBackReferenceType() && step.getDestinationClassNames().contains(eventClass) &&
                    writtenInstance.getFieldNames().contains(ReservedFields.DELETED)) {
                    //good old backref delete case
                    rootLocator.locateViewRoots(binding.getViewClassName(), tenantIdAndCentricId, globalTenantIdAndCentricId,
                            instanceId, pathIdx, path, modificationStream);
                }
            }
        }
    }

    private Set<String> getAllStepFields(ModelPathStep step) {
        Set<String> allStepFields = new HashSet<>();
        if (step.getStepType().equals(ModelPathStepType.value)) {
            allStepFields.addAll(step.getFieldNames());
        } else {
            allStepFields.add(step.getRefFieldName());
        }
        allStepFields.add(ReservedFields.DELETED);

        return allStepFields;
    }

    private Set<ObjectId> buildAffectedBackReferencedObjects(EventWrite write, String refField, Id centricId) {
        Set<ObjectId> affected = new HashSet<>();
        affected.addAll(write.getDereferencedObjects(centricId, refField));
        affected.addAll(write.getReferencedObjects(centricId, refField));

        return affected;
    }
}
