package com.jivesoftware.os.tasmo.lib.ingress;

import com.jivesoftware.os.jive.utils.base.interfaces.CallbackStream;
import com.jivesoftware.os.jive.utils.id.ObjectId;
import com.jivesoftware.os.jive.utils.id.TenantIdAndCentricId;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import com.jivesoftware.os.tasmo.lib.read.ReadMaterializerViewFields;
import com.jivesoftware.os.tasmo.lib.write.PathId;
import com.jivesoftware.os.tasmo.lib.write.ViewField;
import com.jivesoftware.os.tasmo.view.notification.api.ViewNotification;
import com.jivesoftware.os.tasmo.view.reader.api.ViewDescriptor;
import com.jivesoftware.os.tasmo.view.reader.service.shared.ViewValue;
import com.jivesoftware.os.tasmo.view.reader.service.writer.ViewValueWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 *
 * @author jonathan.colt
 */
public class TasmoNotificationsIngress implements CallbackStream<List<ViewNotification>> {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final OrderIdProvider threadTimestamp;
    private final ReadMaterializerViewFields readMaterializer;
    private final ViewValueWriter viewValueWriter;

    public TasmoNotificationsIngress(OrderIdProvider threadTimestamp,
        ReadMaterializerViewFields readMaterializer,
        ViewValueWriter viewValueWriter) {
        this.threadTimestamp = threadTimestamp;
        this.readMaterializer = readMaterializer;
        this.viewValueWriter = viewValueWriter;
    }

    @Override
    public List<ViewNotification> callback(List<ViewNotification> viewNotifications) throws Exception {
        List<ViewNotification> failedToProcess = new ArrayList<>();
        if (viewNotifications == null) {
            return failedToProcess;
        }

        List<ViewDescriptor> viewDescriptors = new ArrayList<>(viewNotifications.size());
        for (ViewNotification viewNotification : viewNotifications) {
            viewDescriptors.add(new ViewDescriptor(viewNotification.getTenantIdAndCentricId(),
                viewNotification.getActorId(),
                viewNotification.getViewId()));
        }
        
        long threadTime = threadTimestamp.nextId();
        Map<ViewDescriptor, List<ViewField>> readMaterialized = readMaterializer.readMaterialize(viewDescriptors);
        for (Entry<ViewDescriptor, List<ViewField>> changeSets : readMaterialized.entrySet()) {
            ViewDescriptor viewDescriptor = changeSets.getKey();
            List<ViewField> changes = changeSets.getValue();
            TenantIdAndCentricId tenantIdAndCentricId = viewDescriptor.getTenantIdAndCentricId();

            ViewValueWriter.Transaction transaction = viewValueWriter.begin(tenantIdAndCentricId);
            for (ViewField change : changes) {
                if (change.getType() == ViewField.ViewFieldChangeType.add) {
                    PathId[] modelPathInstanceIds = change.getModelPathInstanceIds();
                    ObjectId[] ids = new ObjectId[modelPathInstanceIds.length];
                    for (int i = 0; i < modelPathInstanceIds.length; i++) {
                        ids[i] = modelPathInstanceIds[i].getObjectId();
                    }
                    transaction.set(change.getViewObjectId(),
                        change.getModelPathIdHashcode(),
                        ids,
                        new ViewValue(change.getModelPathTimestamps(), change.getValue()),
                        threadTime);
                }
            }
            viewValueWriter.commit(transaction);
            viewValueWriter.clear(tenantIdAndCentricId, viewDescriptor.getViewId(), threadTime - 1);
        }

        LOG.warn("TODO Failed to process isn't implemented.");
        return failedToProcess;
    }

}
