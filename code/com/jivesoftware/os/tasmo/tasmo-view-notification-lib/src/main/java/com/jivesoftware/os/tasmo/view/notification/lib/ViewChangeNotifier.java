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

import com.jivesoftware.os.jive.utils.base.interfaces.CallbackStream;
import com.jivesoftware.os.tasmo.id.Id;
import com.jivesoftware.os.tasmo.id.ObjectId;
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
            viewChangeNotificationProcessor.process(detectModifiedViews(write), write.getWrittenEvent());
        }
    }

    private ModifiedViewProvider detectModifiedViews(EventWrite write) {
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

    private void detectChangedViewsForBinding(EventWrite write, ViewBinding binding, CallbackStream<ModifiedViewInfo> modificationStream) {
        WrittenEvent event = write.getWrittenEvent();
        WrittenInstance writtenInstance = event.getWrittenInstance();
        ObjectId instanceId = writtenInstance.getInstanceId();
        String eventClass = instanceId.getClassName();
        Id actorId = event.getActorId();
        Id centricId = actorId.equals(event.getCentricId()) ? Id.NULL : event.getCentricId();
        TenantIdAndCentricId tenantIdAndCentricId = new TenantIdAndCentricId(event.getTenantId(), centricId);


        //TODO handle global and centric, etc and pull dereferenced objects out of event write
        for (ModelPath path : binding.getModelPaths()) {
            for (ModelPathStep step : path.getPathMembers()) {
                if (step.getOriginClassNames().contains(eventClass)) {
                    for (String fieldName : event.getWrittenInstance().getFieldNames()) {
                        if (step.getStepType().equals(ModelPathStepType.ref) || step.getStepType().equals(ModelPathStepType.refs)) {
                            if (step.getRefFieldName().equals(fieldName)) {
                                //build chain to root from step above starting with get_aids if above is ref, get_bids if backref
                                //need instance id, ref field name of step above, path, and step above
                                //if root, just add view id
                                rootLocator.locateViewRoots(tenantIdAndCentricId, actorId,
                                    instanceId, step, path, new ViewModficationStream(tenantIdAndCentricId, modificationStream));

                            }
                        } else if (step.getStepType().equals(ModelPathStepType.value)) {
                            if (step.getFieldNames().contains(fieldName)) {
                                //build chain to root from step above starting with get_aids if above is ref, get_bids if backref
                                //need instance id, ref field name of step above, path, and step above
                                //if root, just add view id
                                rootLocator.locateViewRoots(tenantIdAndCentricId, actorId,
                                    instanceId, step, path, new ViewModficationStream(tenantIdAndCentricId, modificationStream));
                            }
                        } else {
                            //all backref types
                            if (step.getRefFieldName().equals(fieldName)) {
                                //build chain to root from this step above starting with get_aids
                                //need instance id, ref field name of step, path, and step
                                //if root, just add view id for each bid in the field value
                                //problem - by the time this is run, the old relationships are toast, so we can't notify of removal
                                rootLocator.locateViewRoots(tenantIdAndCentricId, actorId,
                                    instanceId, step, path, new ViewModficationStream(tenantIdAndCentricId, modificationStream));
                            }
                        }
                    }
                }
            }

        }
    }

    private static class ViewModficationStream implements CallbackStream<ObjectId> {

        private final CallbackStream<ModifiedViewInfo> infoStream;
        private final TenantIdAndCentricId tenantIdAndCentricId;

        public ViewModficationStream(TenantIdAndCentricId tenantIdAndCentricId, CallbackStream<ModifiedViewInfo> infoStream) {
            this.tenantIdAndCentricId = tenantIdAndCentricId;
            this.infoStream = infoStream;
        }

        @Override
        public ObjectId callback(ObjectId value) throws Exception {
            ModifiedViewInfo viewInfo = null;
            if (value != null) {
                viewInfo = new ModifiedViewInfo(tenantIdAndCentricId, value);
            }

            infoStream.callback(viewInfo);

            return value;
        }
    }
}
