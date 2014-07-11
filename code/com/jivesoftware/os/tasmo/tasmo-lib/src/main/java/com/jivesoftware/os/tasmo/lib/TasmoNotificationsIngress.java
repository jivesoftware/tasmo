package com.jivesoftware.os.tasmo.lib;

import com.jivesoftware.os.jive.utils.base.interfaces.CallbackStream;
import com.jivesoftware.os.jive.utils.id.ObjectId;
import com.jivesoftware.os.jive.utils.id.TenantIdAndCentricId;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import com.jivesoftware.os.tasmo.lib.read.ReadMaterializer;
import com.jivesoftware.os.tasmo.lib.write.PathId;
import com.jivesoftware.os.tasmo.lib.write.ViewFieldChange;
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
    private final ReadMaterializer readMaterializer;
    private final ViewValueWriter viewValueWriter;

    public TasmoNotificationsIngress(OrderIdProvider threadTimestamp,
        ReadMaterializer readMaterializer,
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
        Map<ViewDescriptor, List<ViewFieldChange>> readMaterialized = readMaterializer.readMaterialize(viewDescriptors);
        for (Entry<ViewDescriptor, List<ViewFieldChange>> changeSets : readMaterialized.entrySet()) {
            ViewDescriptor viewDescriptor = changeSets.getKey();
            List<ViewFieldChange> changes = changeSets.getValue();
            TenantIdAndCentricId tenantIdAndCentricId = viewDescriptor.getTenantIdAndCentricId();
            LOG.inc("commitChanges", changes.size());
            LOG.startTimer("commitChanges");

            ViewValueWriter.Transaction transaction = viewValueWriter.begin(tenantIdAndCentricId);
            for (ViewFieldChange change : changes) {
                if (change.getType() == ViewFieldChange.ViewFieldChangeType.add) {
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
            // TODO remove every thing from viewValueStore that is less than threadTime

            LOG.inc("commitedChanges", changes.size());
            LOG.trace("Committed changes to hbase: {}", changes);
        }

        LOG.warn("TODO Failed to process isn't implemented.");
        return failedToProcess;
    }

}
