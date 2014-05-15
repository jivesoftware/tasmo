/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.jivesoftware.os.tasmo.lib.process.bookkeeping;

import com.google.common.base.Optional;
import com.jivesoftware.os.jive.utils.base.interfaces.CallbackStream;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProviderImpl;
import com.jivesoftware.os.tasmo.id.Id;
import com.jivesoftware.os.tasmo.id.TenantId;
import com.jivesoftware.os.tasmo.model.process.WrittenEvent;
import com.jivesoftware.os.tasmo.model.process.WrittenInstance;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 *
 */
public class TasmoEventBookkeeperTest {

    private final OrderIdProvider idProvider = new OrderIdProviderImpl(45);

    @Test
    public void testBooKeeping() throws Exception {
        final List<BookkeepingEvent> callbackVal = new ArrayList<>();

        CallbackStream<List<BookkeepingEvent>> callback = new CallbackStream<List<BookkeepingEvent>>() {
            @Override
            public List<BookkeepingEvent> callback(List<BookkeepingEvent> value) throws Exception {
                callbackVal.addAll(value);
                return value;
            }
        };
        TasmoEventsProcessedNotifier processedNotifier = new TasmoEventsProcessedNotifier(callback);

        List<WrittenEvent> events = getEvents(35);

        processedNotifier.notify(events, Collections.<WrittenEvent>emptyList());

        Assert.assertEquals(callbackVal.size(), events.size());

        Set<Long> ids = new HashSet<>();
        for (BookkeepingEvent bookkeepingEvent : callbackVal) {
            Assert.assertTrue(bookkeepingEvent.isSuccessful());
            Assert.assertTrue(ids.add(bookkeepingEvent.getEventId()));
        }

        callbackVal.clear();

        events = getEvents(3);

        processedNotifier.notify(events, Collections.<WrittenEvent>emptyList());

        Assert.assertEquals(callbackVal.size(), events.size());

        for (BookkeepingEvent bookkeepingEvent : callbackVal) {
            Assert.assertTrue(bookkeepingEvent.isSuccessful());
            Assert.assertTrue(ids.add(bookkeepingEvent.getEventId()));
        }

        callbackVal.clear();

        events = getEvents(456);

        processedNotifier.notify(Collections.<WrittenEvent>emptyList(), events);

        Assert.assertEquals(callbackVal.size(), events.size());

        for (BookkeepingEvent bookkeepingEvent : callbackVal) {
            Assert.assertFalse(bookkeepingEvent.isSuccessful());
            Assert.assertTrue(ids.add(bookkeepingEvent.getEventId()));
        }

    }

    private List<WrittenEvent> getEvents(int numEvents) {
        List<WrittenEvent> events = new ArrayList<>();
        for (int i = 0; i < numEvents; i++) {
            events.add(genEvent());
        }
        return events;

    }

    private WrittenEvent genEvent() {
        final long eventId = idProvider.nextId();

        return new WrittenEvent() {
            @Override
            public long getEventId() {
                return eventId;
            }

            @Override
            public Id getActorId() {
                return new Id(234);
            }

            @Override
            public TenantId getTenantId() {
                return new TenantId("booya");
            }

            @Override
            public Id getCentricId() {
                return getActorId();
            }

            @Override
            public WrittenInstance getWrittenInstance() {
                return null;
            }

            @Override
            public boolean isBookKeepingEnabled() {
                return true;
            }

            @Override
            public Optional<String> getCorrelationId() {
                return Optional.absent();
            }
        };
    }
}
