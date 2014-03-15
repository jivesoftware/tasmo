/*
 * $Revision$
 * $Date$
 *
 * Copyright (C) 1999-$year$ Jive Software. All rights reserved.
 *
 * This software is the proprietary information of Jive Software. Use is subject to license terms.
 */
package com.jivesoftware.os.tasmo.lib;

import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.tasmo.configuration.EventsModel;
import com.jivesoftware.os.tasmo.configuration.events.TenantEventsProvider;
import com.jivesoftware.os.tasmo.configuration.events.VersionedEventsModel;
import com.jivesoftware.os.tasmo.id.TenantId;
import com.jivesoftware.os.tasmo.lib.events.EventValueStore;
import com.jivesoftware.os.tasmo.lib.process.EventProcessorDispatcher;
import com.jivesoftware.os.tasmo.lib.process.RefProcessor;
import com.jivesoftware.os.tasmo.lib.process.ValueProcessor;
import com.jivesoftware.os.tasmo.lib.process.WrittenInstanceHelper;
import com.jivesoftware.os.tasmo.reference.lib.ReferenceStore;

public class DispatcherProvider {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    private final TenantEventsProvider tenantEventsProvider;
    private final ReferenceStore referenceStore;
    private final EventValueStore eventValueStore;
    private final WrittenInstanceHelper writtenInstanceHelper = new WrittenInstanceHelper();

    public DispatcherProvider(
        TenantEventsProvider tenantEventsProvider,
        ReferenceStore referenceStore,
        EventValueStore eventValueStore) {
        this.referenceStore = referenceStore;
        this.eventValueStore = eventValueStore;
        this.tenantEventsProvider = tenantEventsProvider;
    }

    EventProcessorDispatcher getDispatcher(TenantId tenantId, String eventClassName) throws Exception {
        VersionedEventsModel versionedEventsModel = tenantEventsProvider.getVersionedEventsModel(tenantId);
        if (versionedEventsModel == null) {
            LOG.error("Cannot process an event until a model has been loaded.");
            throw new Exception("Cannot process an event until a model has been loaded.");
        } else if (versionedEventsModel.getEventsModel().getEvent(eventClassName) == null) {
            return null;
        } else {
            EventsModel model = versionedEventsModel.getEventsModel();
            return new EventProcessorDispatcher(
                new ValueProcessor(eventValueStore, model),
                new RefProcessor(writtenInstanceHelper, referenceStore, model));
        }
    }

    public void reloadModels() {
        tenantEventsProvider.reloadModels();
    }

    synchronized public void loadModel(TenantId tenantId) {
        tenantEventsProvider.loadModel(tenantId);
    }
}
