/*
 * Copyright 2014 jonathan.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.jivesoftware.os.tasmo.service;

import com.github.rholder.retry.Retryer;
import com.jivesoftware.os.jive.utils.base.interfaces.CallbackStream;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.tasmo.model.process.WrittenEvent;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

/**
 *
 * @author jonathan
 */
public class EventIngressRetryingCallbackStream implements CallbackStream<List<WrittenEvent>> {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final CallbackStream<List<WrittenEvent>> delegate;
    private final Retryer<Boolean> retryer;

    public EventIngressRetryingCallbackStream(CallbackStream<List<WrittenEvent>> delegate, Retryer<Boolean> retryer) {
        this.delegate = delegate;
        this.retryer = retryer;
    }

    @Override
    public List<WrittenEvent> callback(final List<WrittenEvent> events) throws Exception {

        Callable<Boolean> callable = new Callable<Boolean>() {
            final List<WrittenEvent> batch = new ArrayList<>(events);
            boolean retry = false;

            @Override
            public Boolean call() throws Exception {
                if (retry) {
                    LOG.info("CONSISTENCY retrying these:" + batch);
                }
                try {
                    List<WrittenEvent> callback = delegate.callback(batch);
                    if (callback.isEmpty()) {
                        return true;
                    } else {
                        retry = true;
                        batch.clear();
                        batch.addAll(callback);
                        LOG.info("CONSISTENCY will retry these later:" + batch);
                        return false;
                    }
                } catch (Exception x) {
                    LOG.error("CONSISTENCY THIS SHOULD NEVER HAPPEN:" + x);
                    return true;
                }
            }
        };

        while (Boolean.FALSE.equals(callable.call())) {
            Thread.sleep((int) Math.random() * 1_000);
        }
        return events;
    }
}
