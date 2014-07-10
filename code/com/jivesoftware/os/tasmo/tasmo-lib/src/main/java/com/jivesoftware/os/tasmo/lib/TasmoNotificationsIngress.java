package com.jivesoftware.os.tasmo.lib;

import com.jivesoftware.os.jive.utils.base.interfaces.CallbackStream;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import com.jivesoftware.os.tasmo.lib.read.ReadMaterializer;
import com.jivesoftware.os.tasmo.view.notification.api.ViewNotification;
import com.jivesoftware.os.tasmo.view.reader.api.ViewDescriptor;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author jonathan.colt
 */
public class TasmoNotificationsIngress implements CallbackStream<List<ViewNotification>> {

    private final OrderIdProvider threadTimestamp;
    private final ReadMaterializer readMaterializer;

    public TasmoNotificationsIngress(OrderIdProvider threadTimestamp, ReadMaterializer readMaterializer) {
        this.threadTimestamp = threadTimestamp;
        this.readMaterializer = readMaterializer;
    }

    @Override
    public List<ViewNotification> callback(List<ViewNotification> viewNotifications) throws Exception {
        List<ViewDescriptor> viewDescriptors = new ArrayList<>(viewNotifications.size());
        for (ViewNotification viewNotification : viewNotifications) {
            viewDescriptors.add(new ViewDescriptor(viewNotification.getTenantIdAndCentricId(),
                viewNotification.getActorId(),
                viewNotification.getViewId()));
        }
        return null;
    }

}
