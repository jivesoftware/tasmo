/*
 * $Revision$
 * $Date$
 *
 * Copyright (C) 1999-$year$ Jive Software. All rights reserved.
 *
 * This software is the proprietary information of Jive Software. Use is subject to license terms.
 */
package com.jivesoftware.os.tasmo.configuration.views;

import com.google.common.collect.Maps;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.tasmo.id.ChainedVersion;
import com.jivesoftware.os.tasmo.id.TenantId;
import com.jivesoftware.os.tasmo.model.ViewBinding;
import com.jivesoftware.os.tasmo.model.Views;
import com.jivesoftware.os.tasmo.model.ViewsProcessorId;
import com.jivesoftware.os.tasmo.model.ViewsProvider;
import com.jivesoftware.os.tasmo.model.path.ModelPath;
import com.jivesoftware.os.tasmo.model.path.ViewPathDictionary;
import com.jivesoftware.os.tasmo.model.path.ViewPathKeyProvider;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 *
 * @author jonathan.colt
 */
public class TenantViewsProvider {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    private final TenantId masterTenantId;
    private final ConcurrentHashMap<TenantId, VersionedViewsModel> versionedViewModel;
    private final ViewsProvider viewsProvider;

    public TenantViewsProvider(TenantId masterTenantId, ViewsProvider viewsProvider) {
        this.masterTenantId = masterTenantId;
        this.viewsProvider = viewsProvider;
        this.versionedViewModel = new ConcurrentHashMap<>();
    }

    public void reloadModels() {
        for (TenantId tenantId : versionedViewModel.keySet()) {
            loadModel(tenantId);
        }
    }

    public void loadModel(TenantId tenantId) {
        ChainedVersion currentVersion = viewsProvider.getCurrentViewsVersion(tenantId);
        if (currentVersion == ChainedVersion.NULL) {
            versionedViewModel.put(tenantId, new VersionedViewsModel(currentVersion, null));
        } else {
            VersionedViewsModel currentVersionedViewsModel = versionedViewModel.get(tenantId);
            if (currentVersionedViewsModel == null
                || !currentVersionedViewsModel.getVersion().equals(currentVersion)) {

                Map<String, Map<Integer, PathAndDictionary>> newViewValueBindings = Maps.newHashMap();
                Views views = this.viewsProvider.getViews(new ViewsProcessorId(tenantId, "NotBeingUsedYet"));

                for (ViewBinding viewBinding : views.getViewBindings()) {
                    Map<Integer, PathAndDictionary> got = newViewValueBindings.get(viewBinding.getViewClassName());
                    if (got == null) {
                        got = Maps.newHashMap();
                        newViewValueBindings.put(viewBinding.getViewClassName(), got);
                    }

                    for (ModelPath modelPath : viewBinding.getModelPaths()) {
                        PathAndDictionary pathAndDictionary = new PathAndDictionary(modelPath, new ViewPathDictionary(modelPath, new ViewPathKeyProvider()));
                        got.put(modelPath.getId().hashCode(), pathAndDictionary);
                    }

                }

                versionedViewModel.put(tenantId, new VersionedViewsModel(views.getVersion(), newViewValueBindings));
            } else {
                LOG.debug("Didn't reload because view model versions are equal.");
            }
        }
    }

    public Map<Integer, PathAndDictionary> getViewFieldBinding(TenantId tenantId, String className) {
        if (!versionedViewModel.containsKey(tenantId)) {
            loadModel(tenantId);
        }
        VersionedViewsModel versionedViewsModel = versionedViewModel.get(tenantId);
        if (versionedViewsModel == null || versionedViewsModel.getViewsModel() == null) {
            if (!tenantId.equals(masterTenantId)) {
                versionedViewsModel = versionedViewModel.get(masterTenantId);
            }
        }
        if (versionedViewsModel == null) {
            LOG.error("Need to load the view model by calling loadModel().");
            return null;
        }
        return versionedViewsModel.getViewsModel().get(className);
    }
}
