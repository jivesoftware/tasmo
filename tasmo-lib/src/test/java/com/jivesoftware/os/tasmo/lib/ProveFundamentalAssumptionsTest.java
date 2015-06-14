package com.jivesoftware.os.tasmo.lib;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.jivesoftware.os.jive.utils.base.util.locks.StripingLocksProvider;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import org.testng.annotations.Test;

public class ProveFundamentalAssumptionsTest {

    public static final Random rand = new Random();

    @Test (enabled = false, invocationCount = 1_000, singleThreaded = true, skipFailedInvocations = true)
    public void proofTest(TasmoMaterializerHarness t) throws Exception {

        // The problem
        // 1. We have a graph.
        // 2. We change the graph with events.
        // 3. Any node in the graph can be the root of a view.
        // 4. When a node or edge in the graph changes we need to find all implicated view roots and notify then that they will be changed.
        // The hypothesised solution:
        // 1. In reaction to recieving an event traverse existing graph to all roots and publish notification with recieved events timestamp.
        // 2. Apply changes encompassed in event to graph.
        // 3. Travers updated grpah to all roots and publish notification.
        // 4. Consume root changed notification and read materializae views based on latest state in graph.
    }

    static class View {
        String rootId;

    }



    static interface LinkProcessor {
        void process(String id);
    }

    static class Materializer {
        private final Nodes nodes;
        private final Edges edges;

        Materializer(Nodes nodes, Edges edges) {
            this.nodes = nodes;
            this.edges = edges;
        }

        void event(Event event) {

        }
    }

    static interface Event {

        String instanceId();

    }

    static class NodeWriter implements Writer {

        private final Materializer materializer;
        private final Nodes nodes;
        private final String id;
        private final String fieldName;
        private final List<String> possibleValues;
        String finalValue;

        NodeWriter(Materializer materializer, Nodes nodes, String id, String fieldName, List<String> possibleValues) {
            this.materializer = materializer;
            this.nodes = nodes;
            this.id = id;
            this.fieldName = fieldName;
            this.possibleValues = possibleValues;
        }

        @Override
        public void update() {
        }

        @Override
        public void remove() {
        }

        @Override
        public void done() {
        }

    }

    static class EdgeWriter implements Writer {

        private final Materializer materializer;
        private final Edges edges;
        private final String a;
        private final String fieldName;
        private final List<String> possibleBs;

        EdgeWriter(Materializer materializer, Edges edges, String a, String fieldName, List<String> possibleBs) {
            this.materializer = materializer;
            this.edges = edges;
            this.a = a;
            this.fieldName = fieldName;
            this.possibleBs = possibleBs;
        }

        @Override
        public void update() {
        }

        @Override
        public void remove() {
        }

        @Override
        public void done() {
        }
    }

    static interface Writer {

        void update();

        void remove();

        void done();
    }

    static class Nodes {

        private final StripingLocksProvider<String> locks = new StripingLocksProvider<>(1_024);
        private final ConcurrentHashMap<String, TimestampedValue<String>> values = new ConcurrentHashMap<>();

        void add(String a, String fieldName, String value, long timestamp) {
            String key = a + ":" + fieldName;
            synchronized (locks.lock(key)) {
                if (value == null) {
                    TimestampedValue<String> got = values.get(key);
                    if (got == null || got.timestamp < timestamp) {
                        got = new TimestampedValue<>(timestamp, value, true);
                        values.put(key, got);
                    }
                } else {
                    TimestampedValue<String> got = values.get(key);
                    if (got == null || got.timestamp < timestamp) {
                        got = new TimestampedValue<>(timestamp, value, false);
                        values.put(key, got);
                    }
                }
            }
        }

        TimestampedValue<String> get(String a, String fieldName) {
            return values.get(a + ":" + fieldName);
        }
    }

    static class Edges {

        private final StripingLocksProvider<String> locks = new StripingLocksProvider<>(1_024);
        private final ConcurrentHashMap<String, TimestampedValue<String[]>> forward = new ConcurrentHashMap<>();
        private final Multimap<String, String> backward = HashMultimap.create();

        void link(String a, String fieldName, String[] bs, long timestamp) {
            String key = a + ":" + fieldName;
            synchronized (locks.lock(key)) {
                if (bs == null) {
                    TimestampedValue<String[]> got = forward.get(key);
                    if (got == null || got.timestamp < timestamp) {
                        if (got != null && !got.tombstone) {
                            for (String b : got.value) {
                                String bkey = b + ":" + fieldName;
                                backward.remove(bkey, a);
                            }
                        }
                        got = new TimestampedValue<>(timestamp, bs, true);
                        forward.put(key, got);
                    }
                } else {
                    TimestampedValue<String[]> got = forward.get(key);
                    if (got == null || got.timestamp < timestamp) {
                        if (got != null && !got.tombstone) {
                            for (String b : got.value) {
                                String bkey = b + ":" + fieldName;
                                backward.remove(bkey, a);
                            }
                        }
                        got = new TimestampedValue<>(timestamp, bs, false);
                        forward.put(key, got);
                        for (String b : bs) {
                            String bkey = b + ":" + fieldName;
                            backward.put(bkey, a);
                        }
                    }
                }
            }
        }

        String[] forward(String a, String aFieldName) {
            return null;
        }

        String[] backward(String b, String aFieldName) {
            return null;
        }
    }

    static class TimestampedValue<V> {

        final long timestamp;
        final V value;
        final boolean tombstone;

        TimestampedValue(long timestamp, V value, boolean tombstone) {
            this.timestamp = timestamp;
            this.value = value;
            this.tombstone = tombstone;
        }
    }
}
