package com.jivesoftware.os.tasmo.view.reader.api;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.jivesoftware.os.tasmo.id.Id;
import com.jivesoftware.os.tasmo.id.ObjectId;
import com.jivesoftware.os.tasmo.id.TenantId;
import com.jivesoftware.os.tasmo.id.TenantIdAndCentricId;
import java.util.List;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class ViewDescriptorTest {
    private static final ObjectMapper mapper = new ObjectMapper();

    @Test
    public void serialize() throws Exception {
        final ViewDescriptor descriptor = new ViewDescriptor(new TenantIdAndCentricId(new TenantId("test"), Id.NULL),
                new Id(1), new ObjectId("Test", new Id(2)));

        final String serialized = mapper.writeValueAsString(ImmutableList.of(descriptor));

        assertEquals(mapper.<List<ViewDescriptor>>readValue(serialized, new TypeReference<List<ViewDescriptor>>() { }),
            ImmutableList.of(descriptor));
    }
}
