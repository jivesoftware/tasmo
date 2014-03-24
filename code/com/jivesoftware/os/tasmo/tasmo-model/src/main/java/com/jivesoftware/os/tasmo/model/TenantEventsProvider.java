/*
 * $Revision$
 * $Date$
 *
 * Copyright (C) 1999-$year$ Jive Software. All rights reserved.
 *
 * This software is the proprietary information of Jive Software. Use is subject to license terms.
 */
package com.jivesoftware.os.tasmo.model;

import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.tasmo.id.ChainedVersion;
import com.jivesoftware.os.tasmo.id.TenantId;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.lang.mutable.MutableInt;

public class TenantEventsProvider {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    private final EventsProvider eventsProvider;
    private ConcurrentHashMap<TenantId, VersionedEventsModel> versionedEventsModels;
    private final TenantId masterTenantId;

    public TenantEventsProvider(TenantId masterTenantId, EventsProvider eventsProvider) {
        this.masterTenantId = masterTenantId;
        this.eventsProvider = eventsProvider;
        this.versionedEventsModels = new ConcurrentHashMap<>();
    }

    public void reloadModels() {
        for (TenantId tenantId : versionedEventsModels.keySet()) {
            loadModel(tenantId);
        }
    }

    public void loadModel(TenantId tenantId) {
        ChainedVersion currentVersion = eventsProvider.getCurrentEventsVersion(tenantId);
        if (currentVersion == ChainedVersion.NULL) {
            versionedEventsModels.put(tenantId, new VersionedEventsModel(currentVersion, null));
        } else {
            VersionedEventsModel currentVersionedEventModel = versionedEventsModels.get(tenantId);
            if (currentVersionedEventModel == null
                || !currentVersionedEventModel.getVersion().equals(currentVersion)) {

                final MutableInt errors = new MutableInt();
                final EventsModel newEventsModel = new EventsModel();
                List<EventDefinition> events = eventsProvider.getEvents(new EventsProcessorId(tenantId, "NotBeingUsedYet"));
                for (EventDefinition event : events) {
                    try {
                        newEventsModel.addEvent(event);
                    } catch (Exception x) {
                        LOG.error("Failed to load event for " + event, x);
                        throw new RuntimeException("Failed to load (" + errors.longValue() + ") event/s. ");
                    }
                }
                versionedEventsModels.put(tenantId, new VersionedEventsModel(currentVersion, newEventsModel));

            } else {
                LOG.debug("Didn't reload because event model versions are equal.");
            }
        }
    }

    public VersionedEventsModel getVersionedEventsModel(TenantId tenantId) {
        if (!versionedEventsModels.containsKey(tenantId)) {
            loadModel(tenantId);
        }
        VersionedEventsModel eventsModel = versionedEventsModels.get(tenantId);
        if (eventsModel == null || eventsModel.getEventsModel() == null) {
            if (!tenantId.equals(masterTenantId)) {
                eventsModel = versionedEventsModels.get(masterTenantId);
            }
        }
        return eventsModel;
    }
}
