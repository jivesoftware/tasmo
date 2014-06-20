package com.jivesoftware.os.tasmo.reference.reader.api;

import com.jivesoftware.os.jive.utils.id.Id;
import com.jivesoftware.os.jive.utils.id.ObjectId;
import com.jivesoftware.os.jive.utils.id.TenantId;
import java.util.List;

/**
 * Provides methods for reading references
 * @param <V>
 * @param <E>
 */
public interface ReferencesReader<V, E extends Throwable> {

    List<V> getReferencedBy(TenantId tenantId,
            Id userId,
            ObjectId pointOfReference,
            String nameOfReferencingEventClass,
            String nameOfReferencingEventField,
            ObjectId nextBatchStart,
            int resultCount) throws E;

}
