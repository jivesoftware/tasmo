package com.jivesoftware.os.tasmo.test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.jivesoftware.os.jive.utils.base.interfaces.CallbackStream;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.jive.utils.row.column.value.store.api.ColumnValueAndTimestamp;
import com.jivesoftware.os.jive.utils.row.column.value.store.api.TenantIdAndRow;
import com.jivesoftware.os.tasmo.event.api.JsonEventConventions;
import com.jivesoftware.os.tasmo.event.api.write.Event;
import com.jivesoftware.os.tasmo.id.Id;
import com.jivesoftware.os.tasmo.id.ImmutableByteArray;
import com.jivesoftware.os.tasmo.id.ObjectId;
import com.jivesoftware.os.tasmo.id.TenantIdAndCentricId;
import com.jivesoftware.os.tasmo.model.path.ModelPathStep;
import com.jivesoftware.os.tasmo.model.path.ModelPathStepType;
import com.jivesoftware.os.tasmo.reference.lib.concur.PathConsistencyException;
import com.jivesoftware.os.tasmo.view.reader.service.shared.ViewValue;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;
import org.testng.Assert;

/**
 *
 * @author jonathan
 */
public class AssertInputCase {

    private final MetricLogger LOG = MetricLoggerFactory.getLogger();
    private final Executor executor;
    private final long seed;
    private final TenantIdAndCentricId tenantIdAndCentricId;
    private final Id actorId;
    private final int maxFanOut;
    private final boolean verbose;

    public AssertInputCase(Executor executor, long seed, TenantIdAndCentricId tenantIdAndCentricId, Id actorId, int maxFanOut, boolean verbose) {
        this.executor = executor;
        this.seed = seed;
        this.tenantIdAndCentricId = tenantIdAndCentricId;
        this.actorId = actorId;
        this.maxFanOut = maxFanOut;
        this.verbose = verbose;
    }

    public void assertCombination(final AssertableCase ic, Long onlyRunTestId, boolean multiThreadWrites) throws Throwable {
        try {
            if (ic.testId % 1000 == 0) {
                if (ic.testId == 0) {
                    System.out.println("***** Begin test category:" + ic.category + " multi-threaded:" + multiThreadWrites + " ********");
                } else {
                    System.out.println("***** Ran " + ic.testId + " for category:" + ic.category + " multi-threaded:" + multiThreadWrites + " ********");
                }
            }
            if (onlyRunTestId != null && onlyRunTestId != ic.testId) {
                return;
            }
            println("***** category:" + ic.category + " testId:" + ic.testId + " BINDING *****");
            println(ic.binding);
            Expectations expectations = ic.materialization.initModelPaths(tenantIdAndCentricId.getTenantId(), Arrays.asList(ic.binding));

            List<Event> firedEvents = ic.input.getFiredEvents();
            println("***** category:" + ic.category + " testId:" + ic.testId + " EVENTS (" + firedEvents.size() + ") *****");
            for (Event evt : firedEvents) {
                println(evt);
            }

            println("***** category:" + ic.category + " testId:" + ic.testId + " FIRING EVENTS (" + firedEvents.size() + ") *****");
            if (multiThreadWrites) {
                fireEventInParallel(firedEvents, ic);
            } else {
                for (Event evt : firedEvents) {
                    ic.eventWriterProvider.eventWriter().write(evt);
                }
            }

            println("***** category:" + ic.category + " testId:" + ic.testId + " BUILDING ASSERTIONS *****");
            expectations.buildExpectations(ic.testId, expectations, ic.binding, ic.input, ic.deletedId);

            println("***** category:" + ic.category + " testId:" + ic.testId + " ASSERTING *****");
            expectations.assertExpectation(tenantIdAndCentricId);
            expectations.clear();

            println("***** category:" + ic.category + " testId:" + ic.testId + " OUTPUT *****");
            ObjectNode view = ic.materialization.readView(tenantIdAndCentricId, actorId, ic.input.getViewId());
            println(ic.materialization.mapper.writeValueAsString(view));

            if (!ic.category.equals("removes")) {
                List<AssertionResult> allBranchResults = new ArrayList<>();
                assertViewElementExists(ic.binding.getModelPaths().get(0).getPathMembers(), 0, view, ic.input.getLeafNodeFields(), allBranchResults);

                for (AssertionResult result : allBranchResults) {
                    Assert.assertTrue(result.isPassed(), result.getMessage());
                }
            }

            println("***** category:" + ic.category + " testId:" + ic.testId + " PASSED *****");

        } catch (Throwable t) {
            System.out.println("Test:testAllModelPathCombinationsAndEventFireCombinations: category:" + ic.category
                    + " testId:" + ic.testId + " seed:" + seed + " Failed.");
            t.printStackTrace();

            if (verbose) {
                System.out.println(Thread.currentThread() + " |--> testId:" + ic.testId + " seed:" + seed + " Failed.");
                System.out.println(Thread.currentThread() + " |--> REASON ");
                System.out.println(Thread.currentThread() + " |--> " + t.getMessage() + " " + t.getClass());
                System.out.println(Thread.currentThread() + " |--> FAILED " + verbose);

                ic.materialization.rawViewValueStore.getAllRowKeys(100, null, new CallbackStream<TenantIdAndRow<TenantIdAndCentricId, ImmutableByteArray>>() {

                    @Override
                    public TenantIdAndRow<TenantIdAndCentricId, ImmutableByteArray> callback(
                            final TenantIdAndRow<TenantIdAndCentricId, ImmutableByteArray> row) throws Exception {
                        if (row != null) {
                            ic.materialization.rawViewValueStore.getEntrys(row.getTenantId(), row.getRow(), null, Long.MAX_VALUE, 1000, false, null, null,
                                    new CallbackStream<ColumnValueAndTimestamp<ImmutableByteArray, ViewValue, Long>>() {

                                        @Override
                                        public ColumnValueAndTimestamp<ImmutableByteArray, ViewValue, Long> callback(
                                                ColumnValueAndTimestamp<ImmutableByteArray, ViewValue, Long> value) throws Exception {
                                                    if (value != null) {

                                                        System.out.println(" |--> " + rowKey(row.getRow())
                                                                + " | " + columnKey(value.getColumn())
                                                                + " | " + value.getValue() + " | " + value.getTimestamp());
                                                    }
                                                    return value;
                                                }
                                    });
                        }
                        return row;
                    }
                });
                System.exit(0);
            }
            throw t;
        }
    }

    public ObjectId rowKey(ImmutableByteArray viewObjectId) throws IOException {
        return new ObjectId(ByteBuffer.wrap(viewObjectId.getImmutableBytes()));
    }

    private String columnKey(ImmutableByteArray bytes) throws IOException {
        StringBuilder sb = new StringBuilder();
        ByteBuffer buf = ByteBuffer.wrap(bytes.getImmutableBytes());
        int pathHashCode = buf.getInt();
        int classesHashCode = buf.getInt();
        sb.append(pathHashCode).append(".").append(classesHashCode).append("[");
        while (buf.remaining() > 0) {
            byte length = buf.get();
            byte[] id = new byte[length];
            buf.get(id);
            sb.append(new Id(id).toStringForm()).append(" ");
        }
        sb.append(']');
        return sb.toString();
    }

    //TODO use and communicate different leaf node fields per branch.
    public void assertViewElementExists(List<ModelPathStep> path, int pathIndex, ObjectNode viewNode,
            Map<String, String> expectedFieldValues, List<AssertionResult> resultAccumulator) {

        if (viewNode == null) {
            resultAccumulator.add(new AssertionResult(false, "Supplied view node is null"));
        } else if (pathIndex == path.size() - 1) {
            resultAccumulator.add(assertLeafNodeFields(viewNode, expectedFieldValues));
        } else {
            ModelPathStep step = path.get(pathIndex);
            String refField = step.getRefFieldName();
            if (refField == null) {
                throw new IllegalArgumentException("Malformed model path - ref field not present in mid-path element");
            }

            if (ModelPathStepType.backRefs.equals(step.getStepType())) {
                refField = "all_" + refField;
            } else if (ModelPathStepType.latest_backRef.equals(step.getStepType())) {
                refField = "latest_" + refField;
            } else if (ModelPathStepType.count.equals(step.getStepType())) {
                refField = "count_" + refField;
            }

            JsonNode nextNode = viewNode.get(refField);
            if (nextNode == null) {
                resultAccumulator.add(new AssertionResult(false, "No view data exists for path element " + step));
            } else if (nextNode.isArray()) { //handles refs and all_backrefs
                ArrayNode arrayNode = (ArrayNode) nextNode;
                if (arrayNode.size() == 0) {
                    resultAccumulator.add(new AssertionResult(false, "Empty array element in view data for path element " + step));
                } else {
                    for (Iterator<JsonNode> iter = arrayNode.elements(); iter.hasNext();) {
                        JsonNode element = iter.next();
                        if (element.isObject()) {
                            assertViewElementExists(path, pathIndex + 1, (ObjectNode) element, expectedFieldValues, resultAccumulator);
                        } else {
                            resultAccumulator.add(new AssertionResult(false, "Array element view data for path element " + step + " was not an object"));
                        }
                    }
                }
            } else if (nextNode.isObject()) { //handles ref and latest_backref
                assertViewElementExists(path, pathIndex + 1, (ObjectNode) nextNode, expectedFieldValues, resultAccumulator);
            } else if (nextNode.isInt()) { //handles count
                resultAccumulator.add(assertCountField((IntNode) nextNode, refField, maxFanOut));
            } else {
                resultAccumulator.add(new AssertionResult(false, "Element view data for path element " + step + " was an unexpected type: " + nextNode));
            }
        }
    }

    private void println(Object line) {
        if (verbose) {
            LOG.info(line == null ? "null" : line.toString());
        }
    }

    private void fireEventInParallel(List<Event> batch, final AssertableCase ic) throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(batch.size());
        final AtomicLong errors = new AtomicLong();
        final JsonEventConventions jec = new JsonEventConventions();
        for (final Event b : batch) {
            executor.execute(new Runnable() {

                @Override
                public void run() {
                    Thread.currentThread().setName("thread-for-event-" + jec.getEventId(b.toJson()));
                    try {
                        int attempts = 0;
                        int maxAttempts = 3; // TODO expose to config
                        while (attempts < maxAttempts) {
                            attempts++;
                            try {

                                ic.eventWriterProvider.eventWriter().write(Arrays.asList(b));
                                break;
                            } catch (Exception e) {
                                boolean pathModifiedException = false;
                                Throwable t = e;
                                while (t != null) {
                                    if (t instanceof PathConsistencyException) {
                                        pathModifiedException = true;
                                        if (LOG.isTraceEnabled()) {
                                            LOG.trace("** RETRY ** " + t.toString(), t);
                                        }

                                    }
                                    t = t.getCause();
                                }
                                if (pathModifiedException) {
                                    Thread.sleep(100); // TODO is yield a better choice?
                                } else {
                                    throw e;
                                }
                            }
                        }
                        if (attempts >= maxAttempts) {
                            LOG.info("FAILED to reach CONSISTENCY after {} attempts for {}", new Object[]{attempts, b});
                            throw new RuntimeException("Failed to reach stasis after " + maxAttempts + " attempts.");
                        } else {
                            if (attempts > 1) {
                                LOG.warn("CONSISTENCY took {} attempts for {}", new Object[]{attempts, b});
                            }
                        }
                    } catch (Exception ex) {
                        ex.printStackTrace();
                        errors.incrementAndGet();
                    }
                    latch.countDown();
                }
            });
        }
        latch.await();
        if (errors.get() > 0) {
            if (verbose) {
                println("Encountered errors while sending events.");
                System.exit(0);
            }
            Assert.fail("Encountered errors while sending events.");
        }
    }

    private AssertionResult assertCountField(IntNode leaf, String field, int expectedFieldValue) {
        if (leaf.intValue() == expectedFieldValue) {
            return new AssertionResult(true, "");
        } else {
            return new AssertionResult(false, "Unexpected value for field " + field + " - found " + leaf);
        }
    }

    private AssertionResult assertLeafNodeFields(ObjectNode leaf, Map<String, String> expectedFieldValues) {
        for (Map.Entry<String, String> entry : expectedFieldValues.entrySet()) {
            String field = entry.getKey();
            String expectedVal = entry.getValue();

            if (expectedVal == null) {
                throw new IllegalArgumentException("Expected field values cannot be null. Expectation for field " + field + " was null");
            }

            JsonNode value = leaf.get(field);
            if (value == null || value.isNull()) {
                return new AssertionResult(false, "Null value found for field " + field);
            }

            String toTest = value.asText();
            if (expectedVal.equals(toTest)) {
                return new AssertionResult(true, "");
            } else {
                return new AssertionResult(false, "Unexpected value for field " + field + " - found " + toTest);
            }
        }

        return new AssertionResult(true, "");

    }

}
