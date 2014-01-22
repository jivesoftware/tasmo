package com.jivesoftware.os.tasmo.view.reader.service;

import com.jivesoftware.os.jive.utils.row.column.value.store.api.ColumnValueAndTimestamp;
import com.jivesoftware.os.tasmo.id.ImmutableByteArray;
import com.jivesoftware.os.tasmo.view.reader.api.ViewDescriptor;

public interface StaleViewFieldStream {

    void stream(ViewDescriptor viewDescriptor, ColumnValueAndTimestamp<ImmutableByteArray, String, Long> value);
}
