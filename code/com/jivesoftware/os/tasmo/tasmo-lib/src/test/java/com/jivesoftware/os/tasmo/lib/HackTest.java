
package com.jivesoftware.os.tasmo.lib;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.io.Files;
import com.jivesoftware.os.tasmo.event.api.JsonEventConventions;
import com.jivesoftware.os.tasmo.event.api.write.Event;
import com.jivesoftware.os.tasmo.id.Id;
import com.jivesoftware.os.tasmo.id.ObjectId;
import com.jivesoftware.os.tasmo.id.TenantId;
import com.jivesoftware.os.tasmo.id.TenantIdAndCentricId;
import java.io.File;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.testng.annotations.Test;

import static org.testng.Assert.fail;

/**
 *
 */
public class HackTest extends BaseTasmoTest {




    @Test(invocationCount = 10, singleThreaded = true, skipFailedInvocations = true, enabled = false)
    public void testRefUpdates() throws Exception {
//        LogManager.getLogger("com.jivesoftware.os.tasmo").setLevel(Level.TRACE);
//        LogManager.getLogger("com.jivesoftware.os.tasmo.lib.concur.ConcurrencyAndExistanceCommitChange").setLevel(Level.TRACE);
//        LogManager.getLogger("com.jivesoftware.os.tasmo.reference.lib.ReferenceStore").setLevel(Level.TRACE);
//        LogManager.getLogger("com.jivesoftware.os.tasmo.view.reader.service.writer.WriteToViewValueStore").setLevel(Level.TRACE);

        ArrayNode views = mapper.readValue(new URL("file:///jive/all_views.json"), ArrayNode.class);
        final JsonEventConventions eventConventions = new JsonEventConventions();
        Expectations expectations = initModelPaths(views);
        List<String> stringEvents = Files.readLines(new File("/jive/event-log-root.json"), Charset.defaultCharset());
        final Set<Id> commentInstanceIds = new HashSet<>();
        TenantId tenantId = new TenantId("api-tests-npbfqo");
        ExecutorService executorService = Executors.newFixedThreadPool(20);
        for (final String stringEvent : stringEvents) {
            executorService.submit(new Runnable() {
                public void run() {
                    try {
                        final ObjectNode eventObjectNode = mapper.readValue(stringEvent, ObjectNode.class);
                        ObjectId instanceId = eventConventions.getInstanceObjectId(eventObjectNode);
                        write(new Event(eventObjectNode, instanceId));
                        if (instanceId.isClassName("Comment")) {
                            commentInstanceIds.add(instanceId.getId());
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                        throw new RuntimeException(e);
                    }
                }
            });
        }
        Thread.sleep(5000);
        //stringEvents = Files.readLines(new File("/Users/sam.meder/relevant_events.txt"), Charset.defaultCharset());
        System.out.println("Checking " + commentInstanceIds.size() + " views");
        boolean failed = false;
        for (Id commentInstanceId : commentInstanceIds) {
            ObjectId commentAll2View = new ObjectId("CommentAll2View", commentInstanceId);
            ObjectNode view = readView(
                    new TenantIdAndCentricId(tenantId, Id.NULL),
                    actorId, commentAll2View);
            System.err.println("View: " + commentInstanceId.toStringForm());
            System.err.println(mapper.writeValueAsString(view));
            if (view == null) {
                System.err.println("Failed to load view for " + commentAll2View);
            } else {
                if (!view.has("latest_root")) {
                    failed = true;
                }
            }
        }
        if (failed) {
            fail();
        }
    }
}
