package com.jivesoftware.os.tasmo.lib;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.io.Files;
import com.jivesoftware.os.jive.utils.id.Id;
import com.jivesoftware.os.jive.utils.id.ObjectId;
import com.jivesoftware.os.jive.utils.id.TenantIdAndCentricId;
import com.jivesoftware.os.tasmo.event.api.JsonEventConventions;
import com.jivesoftware.os.tasmo.event.api.write.Event;
import com.jivesoftware.os.tasmo.model.Views;
import java.io.File;
import java.nio.charset.Charset;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.testng.annotations.Test;

import static org.testng.Assert.fail;

/**
 *
 */
public class DebugHarnessTest extends BaseTest {

    @Test(invocationCount = 1_000, singleThreaded = false, skipFailedInvocations = true, enabled = false)
    public void hackTest(TasmoMaterializerHarness t) throws Exception {
//        LogManager.getLogger("com.jivesoftware.os.tasmo").setLevel(Level.TRACE);
//        LogManager.getLogger("com.jivesoftware.os.tasmo.lib.concur.ConcurrencyAndExistanceCommitChange").setLevel(Level.TRACE);
//        LogManager.getLogger("com.jivesoftware.os.tasmo.reference.lib.ReferenceStore").setLevel(Level.TRACE);
//        LogManager.getLogger("com.jivesoftware.os.tasmo.view.reader.service.writer.WriteToViewValueStore").setLevel(Level.TRACE);

        ArrayNode rawViews = mapper.readValue(new File("/home/jonathan/jive/os/all_views.json"), ArrayNode.class);
        final JsonEventConventions eventConventions = new JsonEventConventions();
        Views views = TasmoModelFactory.modelToViews(rawViews);
        t.initModel(views);

        List<String> stringEvents = Files.readLines(new File("/home/jonathan/jive/os/events.txt"), Charset.defaultCharset());
        final Set<Id> instanceIds = new HashSet<>();
//        ExecutorService executorService = Executors.newFixedThreadPool(20);
        for (final String stringEvent : stringEvents) {
//            executorService.submit(new Runnable() {
//                public void run() {
            try {
                ObjectNode eventObjectNode = mapper.readValue(stringEvent, ObjectNode.class);
                //System.out.println("eventObjectNode:"+eventObjectNode);
                ObjectId instanceId = eventConventions.getInstanceObjectId(eventObjectNode);
                t.write(new Event(eventObjectNode, instanceId));
                if (instanceId.isClassName("UserFollowActivity")) {
                    instanceIds.add(instanceId.getId());
                }
            } catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
//                }
//            });
        }
        //Thread.sleep(5000);
        System.out.println("Checking " + instanceIds.size() + " views");
        boolean failed = false;
        for (Id commentInstanceId : instanceIds) {
            ObjectId viewObjectId = new ObjectId("UserFollowActivityView", commentInstanceId);
            ObjectNode view = t.readView(
                    new TenantIdAndCentricId(tenantId, Id.NULL),
                    actorId, viewObjectId);
            System.err.println("View: " + commentInstanceId.toStringForm());
            System.err.println(mapper.writeValueAsString(view));
            if (view == null) {
                System.err.println("Failed to load view for " + viewObjectId);
            } else {
                //UserFollowActivityView.verbSubject.followedObject.instanceId
                if (!view.has("verbSubject")) {
                    JsonNode got = view.get("verbSubject");
                    if (!got.has("followedObject")) {
                        got = got.get("followedObject");
                        if (!got.has("instanceId")) {
                            System.out.println("ERROR");
                        } else {
                            failed = true;
                        }
                    } else {
                        failed = true;
                    }
                } else {
                    failed = true;
                }
            }
        }
        if (failed) {
            fail();
        }
    }
}
