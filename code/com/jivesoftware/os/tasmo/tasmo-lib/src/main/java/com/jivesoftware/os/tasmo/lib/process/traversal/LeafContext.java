package com.jivesoftware.os.tasmo.lib.process.traversal;

import com.jivesoftware.os.jive.utils.id.ObjectId;
import com.jivesoftware.os.jive.utils.row.column.value.store.api.ColumnValueAndTimestamp;
import com.jivesoftware.os.tasmo.lib.process.WrittenEventContext;
import com.jivesoftware.os.tasmo.model.process.OpaqueFieldValue;
import com.jivesoftware.os.tasmo.reference.lib.ReferenceWithTimestamp;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 *
 * @author jonathan.colt
 */
public interface LeafContext {

    List<ReferenceWithTimestamp> removeLeafNodeFields(WrittenEventContext writtenEventContext, PathContext pathContext);

    List<ReferenceWithTimestamp> populateLeafNodeFields(
        WrittenEventContext writtenEventContext,
        PathContext pathContext,
        ObjectId objectInstanceId,
        Set<String> fieldNames,
        Map<String, ColumnValueAndTimestamp<String, OpaqueFieldValue, Long>> fieldValues);

    byte[] toBytes() throws IOException;
}
