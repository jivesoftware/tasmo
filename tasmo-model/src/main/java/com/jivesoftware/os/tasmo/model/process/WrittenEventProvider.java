package com.jivesoftware.os.tasmo.model.process;

import com.jivesoftware.os.jive.utils.row.column.value.store.marshall.api.TypeMarshaller;

/**
 * Entry point for event implementations. Operations provided here are how the tasmo converts between an external event representation and the
 * WrittenEvent API.
 */
public interface WrittenEventProvider<E, V> {

    WrittenEvent convertEvent(E eventData);

    TypeMarshaller<OpaqueFieldValue> getLiteralFieldValueMarshaller();

    OpaqueFieldValue convertFieldValue(V fieldValue);

    /**
     * Create an opaque field value for use in representing nil values.  The nil value must be positively represented
     * by a concrete values that is non-null.
     */
    OpaqueFieldValue createNilValue();

    LeafNodeFields createLeafNodeFields();
}
