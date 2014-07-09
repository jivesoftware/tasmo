package com.jivesoftware.os.tasmo.lib;

import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.jivesoftware.os.jive.utils.base.interfaces.CallbackStream;
import com.jivesoftware.os.tasmo.lib.process.bookkeeping.BookkeepingEvent;
import com.jivesoftware.os.tasmo.model.process.WrittenEvent;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class TasmoViewMaterializerTest {

    private static final ListeningExecutorService LES = MoreExecutors.listeningDecorator(Executors.newSingleThreadExecutor());
    private CallbackStream<List<BookkeepingEvent>> callbackStream;
    private TasmoEventProcessor tasmoEventProcessor;
    private TasmoBlacklist tasmoBlacklist;
    private TasmoViewMaterializer tasmoViewMaterializer;

    @BeforeMethod
    public void beforeMethod() throws Exception {
        callbackStream = Mockito.mock(CallbackStream.class);
        tasmoEventProcessor = Mockito.mock(TasmoEventProcessor.class);
        tasmoBlacklist = new TasmoBlacklist();
        tasmoViewMaterializer = new TasmoViewMaterializer(callbackStream, tasmoEventProcessor, LES, tasmoBlacklist);
    }

    @Test
    public void process_emptyInput() throws Exception {
        tasmoViewMaterializer.process(Collections.<WrittenEvent>emptyList());
        // We should get here just fine
    }

}