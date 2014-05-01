package com.jivesoftware.os.tasmo.lib;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.jivesoftware.os.tasmo.id.Id;
import com.jivesoftware.os.tasmo.id.ObjectId;
import com.jivesoftware.os.tasmo.id.TenantId;
import com.jivesoftware.os.tasmo.lib.write.PathId;
import com.jivesoftware.os.tasmo.lib.write.ViewFieldChange;
import com.jivesoftware.os.tasmo.reference.lib.ReferenceWithTimestamp;
import com.jivesoftware.os.tasmo.view.reader.service.writer.ViewWriteFieldChange;
import java.io.IOException;
import java.util.Arrays;
import org.testng.Assert;
import org.testng.annotations.Test;

public class WriteChangeSerializationTest {

    @Test(enabled = false) // TODO re-enable
    public void testJsonRoundTrip() throws IOException {

        ObjectMapper mapper = new ObjectMapper();

        TenantId tenant = new TenantId("booya");
        ViewFieldChange.ViewFieldChangeType type = ViewFieldChange.ViewFieldChangeType.add;
        ObjectId viewId = new ObjectId("radical", new Id(234));
        String viewFieldName = "stuff";
        PathId[] modelPathIds = new PathId[]{new PathId(new ObjectId("radParent", new Id(768)), 1)};
        ReferenceWithTimestamp[] modelPathVersionIds = new ReferenceWithTimestamp[]{
            new ReferenceWithTimestamp(new ObjectId("radParent", new Id(768)), "foo", 1)};

        ObjectNode value = mapper.createObjectNode();
        value.put("wow", "this should work");
        value.put("hrm", 5345);

        long now = System.currentTimeMillis();

        ViewFieldChange viewFieldChange = new ViewFieldChange(
                1,
                new Id(1234), type, viewId, viewFieldName, modelPathIds, Arrays.asList(modelPathVersionIds),
                new long[]{1}, mapper.writeValueAsBytes(value), now);

        System.out.println("Serializing:\n" + mapper.writeValueAsString(viewFieldChange));

        byte[] serialized = mapper.writeValueAsBytes(viewFieldChange);
        ViewWriteFieldChange change = mapper.readValue(serialized, 0, serialized.length, ViewWriteFieldChange.class);

        System.out.println("De-serializing:\n" + mapper.writeValueAsString(change));

        Assert.assertNotNull(change);

        Assert.assertEquals(change.getType().name(), viewFieldChange.getType().name());
        boolean what = change.getViewObjectId().equals(viewFieldChange.getViewObjectId());
        Assert.assertEquals(change.getViewObjectId(), viewFieldChange.getViewObjectId());

        Assert.assertTrue(Arrays.equals(change.getModelPathInstanceIds(), viewFieldChange.getModelPathInstanceIds()));

        Assert.assertEquals(change.getValue(), viewFieldChange.getValue());
        Assert.assertEquals(change.getTimestamp(), viewFieldChange.getTimestamp());

    }
}
