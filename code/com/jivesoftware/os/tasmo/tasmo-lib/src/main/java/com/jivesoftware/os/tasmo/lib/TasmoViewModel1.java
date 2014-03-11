/*
 * $Revision$
 * $Date$
 *
 * Copyright (C) 1999-$year$ Jive Software. All rights reserved.
 *
 * This software is the proprietary information of Jive Software. Use is subject to license terms.
 */
package com.jivesoftware.os.tasmo.lib;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Maps;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.tasmo.id.ChainedVersion;
import com.jivesoftware.os.tasmo.id.TenantId;
import com.jivesoftware.os.tasmo.lib.events.EventValueStore;
import com.jivesoftware.os.tasmo.lib.process.FieldProcessor;
import com.jivesoftware.os.tasmo.lib.process.FieldProcessorConfig;
import com.jivesoftware.os.tasmo.lib.process.FieldProcessorFactory;
import com.jivesoftware.os.tasmo.lib.process.InitialStepKey;
import com.jivesoftware.os.tasmo.lib.process.WrittenInstanceHelper;
import com.jivesoftware.os.tasmo.lib.write.CommitChange;
import com.jivesoftware.os.tasmo.model.ViewBinding;
import com.jivesoftware.os.tasmo.model.Views;
import com.jivesoftware.os.tasmo.model.ViewsProcessorId;
import com.jivesoftware.os.tasmo.model.ViewsProvider;
import com.jivesoftware.os.tasmo.model.path.ModelPath;
import com.jivesoftware.os.tasmo.model.path.ModelPathStepType;
import com.jivesoftware.os.tasmo.model.process.WrittenEventProvider;
import com.jivesoftware.os.tasmo.reference.lib.ReferenceStore;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class TasmoViewModel1 {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    private final TenantId masterTenantId;
    private final ViewsProvider viewsProvider;
    private final WrittenEventProvider writtenEventProvider;
    private final ConcurrentHashMap<TenantId, VersionedTasmoViewModel> versionedViewModels;
    private final ReferenceStore referenceStore;
    private final EventValueStore eventValueStore;
    private final CommitChange changeWriter;
    private final WrittenInstanceHelper writtenInstanceHelper = new WrittenInstanceHelper();

    public TasmoViewModel1(
        TenantId masterTenantId,
        ViewsProvider viewsProvider,
        WrittenEventProvider writtenEventProvider,
        ReferenceStore referenceStore,
        EventValueStore eventValueStore,
        CommitChange changeWriter) {
        this.masterTenantId = masterTenantId;
        this.viewsProvider = viewsProvider;
        this.writtenEventProvider = writtenEventProvider;
        this.referenceStore = referenceStore;
        this.eventValueStore = eventValueStore;
        this.changeWriter = changeWriter;
        this.versionedViewModels = new ConcurrentHashMap<>();
    }

    VersionedTasmoViewModel getVersionedTasmoViewModel(TenantId tenantId) throws Exception {
        if (!versionedViewModels.containsKey(tenantId)) {
            loadModel(tenantId);
        }
        VersionedTasmoViewModel viewsModel = versionedViewModels.get(tenantId);
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

    synchronized public void loadModel(TenantId tenantId) {
        ChainedVersion currentVersion = viewsProvider.getCurrentViewsVersion(tenantId);
        if (currentVersion == ChainedVersion.NULL) {
            versionedViewModels.put(tenantId, new VersionedTasmoViewModel(ChainedVersion.NULL, null));
        } else {
            VersionedTasmoViewModel currentVersionedViewsModel = versionedViewModels.get(tenantId);
            if (currentVersionedViewsModel == null
                || !currentVersionedViewsModel.getVersion().equals(currentVersion)) {

                Views views = viewsProvider.getViews(new ViewsProcessorId(tenantId, "NotBeingUsedYet"));

                if (views != null) {
                    ListMultimap<String, FieldProcessor> fieldProcessors = bindModelPaths(views);
//                    versionedViewModels.put(tenantId, new VersionedTasmoViewModel(views.getVersion(), dispatchers));
                } else {
                    LOG.info("ViewsProvider failed to provide a 'Views' instance for tenantId:" + tenantId);
                }
            } else {
                LOG.debug("Didn't reload because view model versions are equal.");
            }
        }

    }

    private ListMultimap<String, FieldProcessor> bindModelPaths(Views views) throws IllegalArgumentException {

        Map<String, FieldProcessorFactory> allFieldProcessorFactories = Maps.newHashMap();

        ArrayListMultimap<String, FieldProcessor> viewFieldProcessors = ArrayListMultimap.create();

        for (ViewBinding viewBinding : views.getViewBindings()) {

            String viewClassName = viewBinding.getViewClassName();
            String viewIdFieldName = viewBinding.getViewIdFieldName();
            boolean idCentric = viewBinding.isIdCentric();
            boolean isNotificationRequired = viewBinding.isNotificationRequired();


            FieldProcessorConfig executableStepConfig = new FieldProcessorConfig(
                writtenEventProvider,
                changeWriter,
                viewIdFieldName,
                idCentric,
                isNotificationRequired);

            for (ModelPath modelPath : viewBinding.getModelPaths()) {

                String factoryKey = viewBinding.getViewClassName() + "_" + modelPath.getId();
                if (allFieldProcessorFactories.containsKey(factoryKey)) {
                    throw new IllegalArgumentException("you have already created this binding:" + factoryKey);
                }

                FieldProcessorFactory fieldProcessorFactory = new FieldProcessorFactory(viewClassName, modelPath, eventValueStore, referenceStore);

                LOG.info("Bind:{}", factoryKey);
                allFieldProcessorFactories.put(factoryKey, fieldProcessorFactory);

//                FieldProcessor executableStep = fieldProcessorFactory.buildFieldProcessor(executableStepConfig);
//                viewFieldProcessors.put(viewClassName, executableStep);
            }

        }
        
        return viewFieldProcessors;
    }


    private void groupExecutableStepsByClass(
        Map<String, Map<ModelPathStepType, ArrayListMultimap<InitialStepKey, FieldProcessor>>> groupedExecutableSteps,
        List<FieldProcessor> executableSteps) {

        for (FieldProcessor executableStep : executableSteps) {
            for (String className : executableStep.getInitialClassNames()) {
                Map<ModelPathStepType, ArrayListMultimap<InitialStepKey, FieldProcessor>> typedSteps = groupedExecutableSteps.get(className);
                if (typedSteps == null) {
                    typedSteps = Maps.newHashMap();
                    groupedExecutableSteps.put(className, typedSteps);
                }
                ModelPathStepType stepType = executableStep.getInitialModelPathStepType();
                ArrayListMultimap<InitialStepKey, FieldProcessor> steps = typedSteps.get(stepType);
                if (steps == null) {
                    steps = ArrayListMultimap.create();
                    typedSteps.put(stepType, steps);
                }

                String refFieldName = executableStep.getRefFieldName();
                if (refFieldName != null) {
                    steps.put(new InitialStepKey(refFieldName, refFieldName), executableStep);
                }

                for (String fieldName : executableStep.getInitialFieldNames()) {
                    steps.put(new InitialStepKey(fieldName, refFieldName), executableStep);
                }
            }
        }
    }
}
