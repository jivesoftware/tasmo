package com.jivesoftware.os.tasmo.lib.process;

/**
 *
 * @author jonathan
 */
public interface WrittenEventProcessorDecorator {

    WrittenEventProcessor decorateWrittenEventProcessor(WrittenEventProcessor writtenEventProcessor);
}
