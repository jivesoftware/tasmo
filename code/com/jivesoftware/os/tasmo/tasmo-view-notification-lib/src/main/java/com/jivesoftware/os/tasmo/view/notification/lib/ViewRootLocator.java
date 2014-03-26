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
import com.jivesoftware.os.tasmo.model.path.ModelPath;
import com.jivesoftware.os.tasmo.model.path.ModelPathStep;
import com.jivesoftware.os.tasmo.reference.lib.ReferenceStore;

/**
 *
 * @author pete
 */
public class ViewRootLocator {

    private final ReferenceStore referenceStore;

    public ViewRootLocator(ReferenceStore referenceStore) {
        this.referenceStore = referenceStore;
    }

    public void locateViewRoots(TenantIdAndCentricId tenantIdAndCentricId, Id actorId,
        ObjectId id, ModelPathStep step, ModelPath path, CallbackStream<ObjectId> viewRootStream) {
    }
}
