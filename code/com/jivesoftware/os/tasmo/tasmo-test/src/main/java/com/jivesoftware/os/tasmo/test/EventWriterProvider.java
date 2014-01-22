package com.jivesoftware.os.tasmo.test;

import com.jivesoftware.os.tasmo.event.api.write.EventWriter;

/**
 *
 * Hack to allow testng data provider stuff to run before underlying system is really set up.
 */
public interface EventWriterProvider {

    EventWriter eventWriter();
}
