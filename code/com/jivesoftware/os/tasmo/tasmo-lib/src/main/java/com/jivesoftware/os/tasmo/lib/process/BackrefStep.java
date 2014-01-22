/*
 * $Revision$
 * $Date$
 *
 * Copyright (C) 1999-$year$ Jive Software. All rights reserved.
 *
 * This software is the proprietary information of Jive Software. Use is subject to license terms.
 */

package com.jivesoftware.os.tasmo.lib.process;

import com.jivesoftware.os.jive.utils.base.interfaces.CallbackStream;
import com.jivesoftware.os.tasmo.id.TenantIdAndCentricId;
import com.jivesoftware.os.tasmo.model.path.ModelPathStep;
import com.jivesoftware.os.tasmo.model.process.WrittenEvent;
import com.jivesoftware.os.tasmo.reference.lib.A_IdsStreamer;
import com.jivesoftware.os.tasmo.reference.lib.RefStreamer;
import com.jivesoftware.os.tasmo.reference.lib.Reference;
import com.jivesoftware.os.tasmo.reference.lib.ReferenceStore;
import java.util.Set;

/**
 *
 * @author jonathan.colt
 */
public class BackrefStep implements ProcessStep {

    private final ModelPathStep initialModelPathMember;
    private final ReferenceStore referenceStore;
    private final Set<String> validDownStreamTypes;

    public BackrefStep(ModelPathStep initialModelPathMember, ReferenceStore referenceStore,
        Set<String> validDownStreamTypes) {
        this.initialModelPathMember = initialModelPathMember;
        this.referenceStore = referenceStore;
        this.validDownStreamTypes = validDownStreamTypes;
    }

    @Override
    public void process(TenantIdAndCentricId tenantIdAndCentricId, WrittenEvent writtenEvent, ViewFieldContext context, Reference objectIntanceId,
        final StepStream streamTo) throws Exception {
        RefStreamer refStreamer = new A_IdsStreamer(referenceStore,
            initialModelPathMember.getOriginClassNames(),
            initialModelPathMember.getRefFieldName());

        refStreamer.stream(tenantIdAndCentricId, objectIntanceId, new CallbackStream<Reference>() {
            @Override
            public Reference callback(Reference aId) throws Exception {
                if (aId != null && isValidDownStreamObject(aId)) {
                    streamTo.stream(aId);
                }
                return aId;
            }
        });
    }

    private boolean isValidDownStreamObject(Reference toStream) {
        return validDownStreamTypes == null || validDownStreamTypes.isEmpty() || validDownStreamTypes.contains(toStream.getObjectId().getClassName());
    }

}
