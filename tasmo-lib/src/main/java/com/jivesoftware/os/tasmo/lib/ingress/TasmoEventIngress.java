package com.jivesoftware.os.tasmo.lib.ingress;

import com.jivesoftware.os.rcvs.api.CallbackStream;
import com.jivesoftware.os.tasmo.model.process.WrittenEvent;
import java.util.List;

/**
 *
 */
public class TasmoEventIngress implements CallbackStream<List<WrittenEvent>> {

    final TasmoWriteMaterializer materializer;

    public TasmoEventIngress(TasmoWriteMaterializer materializer) {
        this.materializer = materializer;
    }

    @Override
    public List<WrittenEvent> callback(List<WrittenEvent> value) throws Exception {
        if (value != null) {
            return materializer.process(value);
        }
        return value;
    }
}
