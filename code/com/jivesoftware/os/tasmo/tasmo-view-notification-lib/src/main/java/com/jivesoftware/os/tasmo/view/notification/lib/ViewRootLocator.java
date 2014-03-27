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
import com.jivesoftware.os.tasmo.id.ObjectId;
import com.jivesoftware.os.tasmo.id.TenantIdAndCentricId;
import com.jivesoftware.os.tasmo.model.path.ModelPath;
import com.jivesoftware.os.tasmo.model.path.ModelPathStep;
import com.jivesoftware.os.tasmo.model.process.ModifiedViewInfo;
import com.jivesoftware.os.tasmo.reference.lib.Reference;
import com.jivesoftware.os.tasmo.view.reader.lib.BatchingReferenceStore;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 *
 * @author pete
 */
public class ViewRootLocator {

    private final BatchingReferenceStore referenceStore;

    public ViewRootLocator(BatchingReferenceStore referenceStore) {
        this.referenceStore = referenceStore;
    }

    public void locateViewRoots(String viewClassName, TenantIdAndCentricId tenantIdAndCentricId, TenantIdAndCentricId globalTenantIdAndCentricId,
        ObjectId id, int indexOfId, ModelPath path, CallbackStream<ModifiedViewInfo> viewRootStream) throws Exception {
        ReferenceStreamer centricStream = new ReferenceStreamer(viewClassName, tenantIdAndCentricId, path, indexOfId, viewRootStream, Arrays.asList(id));
        centricStream.followReferences();
        ReferenceStreamer globalStream = new ReferenceStreamer(viewClassName, globalTenantIdAndCentricId, path, indexOfId, viewRootStream, Arrays.asList(id));
        globalStream.followReferences();
    }

    public void locateViewRoots(String viewClassName, TenantIdAndCentricId tenantIdAndCentricId, TenantIdAndCentricId globalTenantIdAndCentricId,
        Collection<ObjectId> idsCentric, Collection<ObjectId> idsGlobal, int idxOfIds, ModelPath path,
        CallbackStream<ModifiedViewInfo> viewRootStream) throws Exception {
        ReferenceStreamer centricStream = new ReferenceStreamer(viewClassName, tenantIdAndCentricId, path, idxOfIds, viewRootStream, idsCentric);
        centricStream.followReferences();
        ReferenceStreamer globalStream = new ReferenceStreamer(viewClassName, globalTenantIdAndCentricId, path, idxOfIds, viewRootStream, idsGlobal);
        globalStream.followReferences();
    }

    private class ReferenceStreamer implements CallbackStream<Reference> {

        private final String viewClassName;
        private final TenantIdAndCentricId tenantIdAndCentricId;
        private final ModelPath path;
        private final int pathIdx;
        private final CallbackStream<ModifiedViewInfo> viewRootStream;
        private final Collection<ObjectId> knownIds;
        private final Set<ObjectId> foundIds = new HashSet<>();
        private final ReferenceStreamer nextStreamer;

        public ReferenceStreamer(String viewClassName, TenantIdAndCentricId tenantIdAndCentricId, ModelPath path, int pathIdx,
            CallbackStream<ModifiedViewInfo> viewRootStream, Collection<ObjectId> knownIds) {
            this.viewClassName = viewClassName;
            this.tenantIdAndCentricId = tenantIdAndCentricId;
            this.path = path;
            this.pathIdx = pathIdx;
            this.viewRootStream = viewRootStream;
            this.knownIds = knownIds;
            this.nextStreamer =
                pathIdx > 0 ? new ReferenceStreamer(viewClassName, tenantIdAndCentricId, path, pathIdx - 1, viewRootStream, foundIds) : null;
        }

        public void followReferences() throws Exception {
            if (pathIdx == 0) {
                emitChangedViews(knownIds);
            } else {
                ModelPathStep step = path.getPathMembers().get(pathIdx - 1);
                for (ObjectId referringId : knownIds) {
                    addReferenceRequest(referringId, step, nextStreamer);
                }
                referenceStore.executeBatch();
            }

        }

        @Override
        public Reference callback(Reference value) throws Exception {
            if (value != null) {
                foundIds.add(value.getObjectId());
            } else if (pathIdx > 0) {
                nextStreamer.followReferences();
            } else {
                emitChangedViews(foundIds);
            }
            return value;

        }

        private void emitChangedViews(Collection<ObjectId> idsToEmit) throws Exception {
            for (ObjectId id : idsToEmit) {
                ObjectId viewRoot = new ObjectId(viewClassName, id.getId());
                viewRootStream.callback(new ModifiedViewInfo(tenantIdAndCentricId, viewRoot));
            }
            viewRootStream.callback(null); //eos
        }

        private void addReferenceRequest(ObjectId id, ModelPathStep step, ReferenceStreamer nextStreamer) {
            if (step.getStepType().isBackReferenceType()) {
                referenceStore.get_bIds(tenantIdAndCentricId, id.getClassName(), step.getRefFieldName(), id, nextStreamer);
            } else {
                referenceStore.get_aIds(tenantIdAndCentricId, id, step.getOriginClassNames(), step.getRefFieldName(), nextStreamer);
            }
        }
    }
}
