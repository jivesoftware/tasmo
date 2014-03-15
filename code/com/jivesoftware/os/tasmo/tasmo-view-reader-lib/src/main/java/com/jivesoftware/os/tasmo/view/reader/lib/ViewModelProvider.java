/*
 * $Revision$
 * $Date$
 *
 * Copyright (C) 1999-$year$ Jive Software. All rights reserved.
 *
 * This software is the proprietary information of Jive Software. Use is subject to license terms.
 */
package com.jivesoftware.os.tasmo.view.reader.lib;

import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.tasmo.id.ChainedVersion;
import com.jivesoftware.os.tasmo.id.ObjectId;
import com.jivesoftware.os.tasmo.id.TenantId;
import com.jivesoftware.os.tasmo.model.ViewBinding;
import com.jivesoftware.os.tasmo.model.Views;
import com.jivesoftware.os.tasmo.model.ViewsProcessorId;
import com.jivesoftware.os.tasmo.model.ViewsProvider;
import com.jivesoftware.os.tasmo.view.reader.api.ViewDescriptor;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ViewModelProvider {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    private final TenantId masterTenantId;
    private final ViewsProvider viewsProvider;
    private final ConcurrentHashMap<TenantId, VersionedTasmoViewModel> versionedViewModels;

    public ViewModelProvider(TenantId masterTenantId, ViewsProvider viewsProvider) {
        this.masterTenantId = masterTenantId;
        this.viewsProvider = viewsProvider;
        this.versionedViewModels = new ConcurrentHashMap<>();
    }
    
    public ViewBinding getBindingForRequest(ViewDescriptor descriptor) {
        VersionedTasmoViewModel viewModel = getModelForTenant(descriptor.getTenantId());
        if (viewModel != null) {
            ObjectId viewId = descriptor.getViewId();
            if (viewId != null) {
                return viewModel.getBinding(viewId.getClassName());
            }
        }
        
        return  null;
    }
    

    private VersionedTasmoViewModel getModelForTenant(TenantId tenantId) {
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

    synchronized private void loadModel(TenantId tenantId) {
        ChainedVersion currentVersion = viewsProvider.getCurrentViewsVersion(tenantId);
        if (currentVersion == ChainedVersion.NULL) {
            versionedViewModels.put(tenantId, new VersionedTasmoViewModel(ChainedVersion.NULL, Collections.<String, ViewBinding>emptyMap()));
        } else {
            VersionedTasmoViewModel currentVersionedViewsModel = versionedViewModels.get(tenantId);
            if (currentVersionedViewsModel == null
                || !currentVersionedViewsModel.getVersion().equals(currentVersion)) {

                Views views = viewsProvider.getViews(new ViewsProcessorId(tenantId, "NotBeingUsedYet"));

                if (views != null) {
                    Map<String, ViewBinding> indexedBindings = indexByView(views);
                    versionedViewModels.put(tenantId, new VersionedTasmoViewModel(currentVersion, indexedBindings));
                } else {
                    LOG.info("ViewsProvider failed to provide a 'Views' instance for tenantId:" + tenantId);
                }
            } else {
                LOG.debug("Didn't reload because view model versions are equal.");
            }
        }
    }
    
    private Map<String, ViewBinding> indexByView(Views views) {
        Map<String, ViewBinding> indexed = new HashMap<>();
        for (ViewBinding binding : views.getViewBindings()) {
            indexed.put(binding.getViewClassName(), binding);
        }
        
        return indexed;
    }

    /*
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
    */
}
