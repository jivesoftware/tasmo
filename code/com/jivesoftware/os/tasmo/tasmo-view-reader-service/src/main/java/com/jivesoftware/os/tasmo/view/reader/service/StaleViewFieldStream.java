package com.jivesoftware.os.tasmo.view.reader.service;

import com.jivesoftware.os.jive.utils.id.ImmutableByteArray;
import com.jivesoftware.os.jive.utils.row.column.value.store.api.ColumnValueAndTimestamp;
import com.jivesoftware.os.tasmo.view.reader.api.ViewDescriptor;
import com.jivesoftware.os.tasmo.view.reader.service.shared.ViewValue;

public interface StaleViewFieldStream {

    void stream(ViewDescriptor viewDescriptor, ColumnValueAndTimestamp<ImmutableByteArray, ViewValue, Long> value);
}
