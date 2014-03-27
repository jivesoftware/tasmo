/*
 * Copyright 2014 pete.
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
package com.jivesoftware.os.tasmo.view.notification.lib;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.tasmo.id.ChainedVersion;
import com.jivesoftware.os.tasmo.id.TenantId;
import com.jivesoftware.os.tasmo.model.ViewBinding;
import com.jivesoftware.os.tasmo.model.Views;
import com.jivesoftware.os.tasmo.model.ViewsProcessorId;
import com.jivesoftware.os.tasmo.model.ViewsProvider;
import com.jivesoftware.os.tasmo.model.path.ModelPath;
import com.jivesoftware.os.tasmo.model.path.ModelPathStep;
import com.jivesoftware.os.tasmo.model.process.WrittenEvent;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 *
 * @author pete
 */
public class NotifiableViewModelProvider {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    private final TenantId masterTenantId;
    private final ViewsProvider viewsProvider;
    private final ConcurrentHashMap<TenantId, VersionedNotifiableViewModel> versionedViewModels;

    public NotifiableViewModelProvider(TenantId masterTenantId, ViewsProvider viewsProvider) {
        this.masterTenantId = masterTenantId;
        this.viewsProvider = viewsProvider;
        this.versionedViewModels = new ConcurrentHashMap<>();
    }

    private VersionedNotifiableViewModel getModelForTenant(TenantId tenantId) {
        if (!versionedViewModels.containsKey(tenantId)) {
            loadModel(tenantId);
        }
        VersionedNotifiableViewModel viewsModel = versionedViewModels.get(tenantId);
        if (viewsModel == null) {
            if (!tenantId.equals(masterTenantId)) {
                viewsModel = versionedViewModels.get(masterTenantId);
            }
        }

        return viewsModel;
    }

    public void reloadModels() {
        for (TenantId tenantId : versionedViewModels.keySet()) {
            loadModel(tenantId);
        }
    }

    synchronized private void loadModel(TenantId tenantId) {
        ChainedVersion currentVersion = viewsProvider.getCurrentViewsVersion(tenantId);
        if (currentVersion == ChainedVersion.NULL) {
            versionedViewModels.put(tenantId, new VersionedNotifiableViewModel(ChainedVersion.NULL, ArrayListMultimap.<String, ViewBinding>create()));
        } else {
            VersionedNotifiableViewModel currentVersionedViewsModel = versionedViewModels.get(tenantId);
            if (currentVersionedViewsModel == null
                || !currentVersionedViewsModel.getVersion().equals(currentVersion)) {

                Views views = viewsProvider.getViews(new ViewsProcessorId(tenantId, "NotBeingUsedYet"));

                if (views != null) {
                    Multimap<String, ViewBinding> indexedBindings = indexByEventClass(views);
                    versionedViewModels.put(tenantId, new VersionedNotifiableViewModel(currentVersion, indexedBindings));
                } else {
                    LOG.info("ViewsProvider failed to provide a 'Views' instance for tenantId:" + tenantId);
                }
            } else {
                LOG.debug("Didn't reload because view model versions are equal.");
            }
        }
    }

    private Multimap<String, ViewBinding> indexByEventClass(Views views) {
        Multimap<String, ViewBinding> index = ArrayListMultimap.create();
        for (ViewBinding binding : views.getViewBindings()) {
            if (binding.isNotificationRequired()) {
                Set<String> eventClasses = new HashSet<>();
                for (ModelPath path : binding.getModelPaths()) {
                    for (ModelPathStep step : path.getPathMembers()) {
                        eventClasses.addAll(step.getOriginClassNames());
                        if (step.getDestinationClassNames() != null) {
                            eventClasses.addAll(step.getDestinationClassNames());
                        }
                    }
                }
                for (String className : eventClasses) {
                    index.put(className, binding);
                }
            }
        }
        return index;
    }

    public Iterable<ViewBinding> getNotifiableBindings(WrittenEvent event) {
        VersionedNotifiableViewModel versionedNotifiableViewModel = getModelForTenant(event.getTenantId());
        if (versionedNotifiableViewModel == null) {
            LOG.warn("No view model found for tenant " + event.getTenantId());
            return Collections.emptyList();
        } else {
            return versionedNotifiableViewModel.getBindings().get(event.getWrittenInstance().getInstanceId().getClassName());
        }
    }
}
