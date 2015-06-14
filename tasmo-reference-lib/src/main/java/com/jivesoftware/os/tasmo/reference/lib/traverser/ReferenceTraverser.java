package com.jivesoftware.os.tasmo.reference.lib.traverser;

import com.jivesoftware.os.jive.utils.id.ObjectId;
import com.jivesoftware.os.jive.utils.id.TenantIdAndCentricId;
import com.jivesoftware.os.rcvs.api.CallbackStream;
import com.jivesoftware.os.tasmo.reference.lib.ReferenceWithTimestamp;
import java.util.Set;

/**
 *
 * @author jonathan
 */
public interface ReferenceTraverser {

    void traverseForwardRef(TenantIdAndCentricId tenantIdAndCentricId,
            Set<String> className,
            String fieldName,
            ObjectId id,
            long threadTimestamp,
            CallbackStream<ReferenceWithTimestamp> refStream) throws InterruptedException, Exception;

    void traversBackRefs(TenantIdAndCentricId tenantIdAndCentricId,
            Set<String> className,
            String fieldName,
            ObjectId id,
            long threadTimestamp,
            CallbackStream<ReferenceWithTimestamp> refStream) throws InterruptedException, Exception;
}
