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
import com.jivesoftware.os.tasmo.lib.process.AllBackrefsProcessor;
import com.jivesoftware.os.tasmo.lib.process.BackrefRemovalProcessor;
import com.jivesoftware.os.tasmo.lib.process.EventProcessorDispatcher;
import com.jivesoftware.os.tasmo.lib.process.ExecutableStep;
import com.jivesoftware.os.tasmo.lib.process.ExecutableStepConfig;
import com.jivesoftware.os.tasmo.lib.process.ExistenceTransitionProcessor;
import com.jivesoftware.os.tasmo.lib.process.FieldProcessorFactory;
import com.jivesoftware.os.tasmo.lib.process.InitialStepKey;
import com.jivesoftware.os.tasmo.lib.process.LatestBackrefProcessor;
import com.jivesoftware.os.tasmo.lib.process.RefProcessor;
import com.jivesoftware.os.tasmo.lib.process.RefRemovalProcessor;
import com.jivesoftware.os.tasmo.lib.process.ValueProcessor;
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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class TasmoViewModel {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    private final TenantId masterTenantId;
    private final ViewsProvider viewsProvider;
    private final WrittenEventProvider writtenEventProvider;
    private final ConcurrentHashMap<TenantId, VersionedTasmoViewModel> versionedViewModels;
    private final ReferenceStore referenceStore;
    private final EventValueStore eventValueStore;
    private final CommitChange changeWriter;
    private final WrittenInstanceHelper writtenInstanceHelper = new WrittenInstanceHelper();

    public TasmoViewModel(
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
        if (viewsModel == null || viewsModel.getDispatchers() == null) {
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
                    ListMultimap<String, EventProcessorDispatcher> dispatchers = bindModelPaths(views);
                    versionedViewModels.put(tenantId, new VersionedTasmoViewModel(views.getVersion(), dispatchers));
                } else {
                    LOG.info("ViewsProvider failed to provide a 'Views' instance for tenantId:" + tenantId);
                }
            } else {
                LOG.debug("Didn't reload because view model versions are equal.");
            }
        }

    }

    private ListMultimap<String, EventProcessorDispatcher> bindModelPaths(Views views) throws IllegalArgumentException {

        Map<String, FieldProcessorFactory> allFieldProcessorFactories = Maps.newHashMap();

        Map<String, Map<ModelPathStepType, ArrayListMultimap<InitialStepKey, ExecutableStep>>> groupSteps = new HashMap<>();
        Map<String, Map<ModelPathStepType, ArrayListMultimap<InitialStepKey, ExecutableStep>>> groupIdCentricSteps = new HashMap<>();

        for (ViewBinding viewBinding : views.getViewBindings()) {

            String viewClassName = viewBinding.getViewClassName();
            String viewIdFieldName = viewBinding.getViewIdFieldName();
            boolean idCentric = viewBinding.isIdCentric();
            boolean isNotificationRequired = viewBinding.isNotificationRequired();

            Map<String, Map<ModelPathStepType, ArrayListMultimap<InitialStepKey, ExecutableStep>>> accumlulate = (idCentric) ? groupIdCentricSteps : groupSteps;

            ExecutableStepConfig executableStepConfig = new ExecutableStepConfig(
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

                List<ExecutableStep> executableSteps = fieldProcessorFactory.buildFieldProcessors(executableStepConfig);
                groupExecutableStepsByClass(accumlulate, executableSteps);

                ExecutableStep initialBackRefStep = fieldProcessorFactory.buildInitialBackrefStep(executableStepConfig);
                if (initialBackRefStep != null) {
                    groupExecutableStepsByClass(accumlulate, Arrays.asList(initialBackRefStep));
                }
            }

        }
        ListMultimap<String, EventProcessorDispatcher> all = ArrayListMultimap.create();
        buildInitialStepDispatchers(groupSteps, false, all);
        buildInitialStepDispatchers(groupIdCentricSteps, true, all);

        return all;
    }

    private void buildInitialStepDispatchers(Map<String, Map<ModelPathStepType, ArrayListMultimap<InitialStepKey, ExecutableStep>>> groupSteps,
        boolean idCentric,
        ListMultimap<String, EventProcessorDispatcher> all) {

        for (String className : groupSteps.keySet()) {
            Map<ModelPathStepType, ArrayListMultimap<InitialStepKey, ExecutableStep>> typedSteps = groupSteps.get(className);

            EventProcessorDispatcher initialStepDispatcher = new EventProcessorDispatcher(className,
                new ValueProcessor(eventValueStore, typedSteps.get(ModelPathStepType.value), idCentric),
                new RefProcessor(writtenInstanceHelper, referenceStore, typedSteps.get(ModelPathStepType.ref), idCentric),
                new RefProcessor(writtenInstanceHelper, referenceStore, typedSteps.get(ModelPathStepType.refs), idCentric),
                new AllBackrefsProcessor(writtenInstanceHelper, referenceStore, typedSteps.get(ModelPathStepType.backRefs), idCentric),
                new LatestBackrefProcessor(writtenInstanceHelper, referenceStore, typedSteps.get(ModelPathStepType.latest_backRef), idCentric),
                new RefRemovalProcessor(referenceStore, typedSteps.get(ModelPathStepType.ref), idCentric),
                new RefRemovalProcessor(referenceStore, typedSteps.get(ModelPathStepType.refs), idCentric),
                new BackrefRemovalProcessor(writtenInstanceHelper, referenceStore, ModelPathStepType.latest_backRef,
                typedSteps.get(ModelPathStepType.latest_backRef), idCentric),
                new BackrefRemovalProcessor(writtenInstanceHelper, referenceStore, ModelPathStepType.backRefs,
                typedSteps.get(ModelPathStepType.backRefs), idCentric),
                new ExistenceTransitionProcessor(typedSteps, idCentric));
            all.put(className, initialStepDispatcher);
        }
    }

    private void groupExecutableStepsByClass(
        Map<String, Map<ModelPathStepType, ArrayListMultimap<InitialStepKey, ExecutableStep>>> groupedExecutableSteps,
        List<ExecutableStep> executableSteps) {

        for (ExecutableStep executableStep : executableSteps) {
            for (String className : executableStep.getInitialClassNames()) {
                Map<ModelPathStepType, ArrayListMultimap<InitialStepKey, ExecutableStep>> typedSteps = groupedExecutableSteps.get(className);
                if (typedSteps == null) {
                    typedSteps = Maps.newHashMap();
                    groupedExecutableSteps.put(className, typedSteps);
                }
                ModelPathStepType stepType = executableStep.getInitialModelPathStepType();
                ArrayListMultimap<InitialStepKey, ExecutableStep> steps = typedSteps.get(stepType);
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
