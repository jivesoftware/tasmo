package com.jivesoftware.os.tasmo.configuration;

import com.google.common.collect.Lists;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.tasmo.model.ViewBinding;
import com.jivesoftware.os.tasmo.model.path.ModelPath;
import com.jivesoftware.os.tasmo.model.path.ModelPathStep;
import com.jivesoftware.os.tasmo.model.path.ModelPathStepType;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

public class BindingGenerator {

    private final MetricLogger LOG = MetricLoggerFactory.getLogger();

    /**
     * @param eventsModel null ok.
     * @param viewModel null not ok.
     */
    public ViewBinding generate(EventsModel eventsModel, ViewModel viewModel) {

        final List<ModelPath> modelPaths = new LinkedList<>();
        PathAccumulator pathAccumulator = new PathAccumulator(new PathAccumulator.AccumlatedPath() {
            @Override
            public void path(List<TypedField> path) {
                String pathId = createPathId(path);
                ModelPath.Builder builder = ModelPath.builder(pathId);
                boolean isRoot = true;
                for (int i = 0; i < path.size(); i++) {
                    TypedField p = path.get(i);
                    TypedField nextTypedField = null;
                    if (i + 1 < path.size()) {
                        nextTypedField = path.get(i + 1);
                    }

                    if (p.getValueType() == ValueType.value) {
                        builder.addPathMember(new ModelPathStep(isRoot,
                            p.getFieldClasses(), null, ModelPathStepType.value, null, Arrays.asList(p.getFieldNames())));
                        ModelPath modelPath = builder.build();
                        LOG.info("Created:" + modelPath);
                        modelPaths.add(modelPath);
                    } else if (nextTypedField != null) {
                        if (p.getValueType() == ValueType.ref) {
                            builder.addPathMember(new ModelPathStep(isRoot,
                                p.getFieldClasses(), p.getFieldNames()[0], ModelPathStepType.ref, nextTypedField.getFieldClasses(), null));
                        } else if (p.getValueType() == ValueType.refs) {
                            builder.addPathMember(new ModelPathStep(isRoot,
                                p.getFieldClasses(), p.getFieldNames()[0], ModelPathStepType.refs, nextTypedField.getFieldClasses(), null));
                        } else if (p.getValueType() == ValueType.backrefs) {
                            builder.addPathMember(new ModelPathStep(isRoot,
                                nextTypedField.getFieldClasses(), p.getFieldNames()[0], ModelPathStepType.backRefs, p.getFieldClasses(), null));
                        } else if (p.getValueType() == ValueType.count) {
                            builder.addPathMember(new ModelPathStep(isRoot,
                                nextTypedField.getFieldClasses(), p.getFieldNames()[0], ModelPathStepType.count, p.getFieldClasses(), null));
                        } else if (p.getValueType() == ValueType.latest_backref) {
                            builder.addPathMember(new ModelPathStep(isRoot,
                                nextTypedField.getFieldClasses(), p.getFieldNames()[0], ModelPathStepType.latest_backRef, p.getFieldClasses(), null));
                        }
                    }
                    isRoot = false;
                }

            }
        });

        if (eventsModel == null) {
            viewModel.getViewObject().depthFirstTraverse(pathAccumulator);
        } else {
            viewModel.getViewObject().depthFirstTraverse(new ValidatingPathCallback(eventsModel, pathAccumulator));
        }
        return new ViewBinding(viewModel.getViewClassName(), modelPaths, false, viewModel.isIdCentric(), viewModel.isNotifiable(),
            viewModel.getViewIdFieldName());
    }

    private String createPathId(List<TypedField> path) {
        StringBuilder pathId = new StringBuilder();
        for (int i = 0; i < path.size(); i++) {
            TypedField typedField = path.get(i);
            if (i + 1 == path.size()) {
                pathId.append(classNamesToString(typedField.getFieldClasses())).append(".").append(typedField.getValueType());
            } else {
                pathId.append(classNamesToString(typedField.getFieldClasses())).append(".").append(typedField.getFieldNames()[0]).
                    append(".").append(typedField.getValueType()).append(".");
            }
        }
        return pathId.toString();
    }

    private String classNamesToString(Set<String> classNames) {
        List<String> classNameList = Lists.newArrayList(classNames);
        Collections.sort(classNameList);

        StringBuilder builder = new StringBuilder();
        String sep = "";
        for (String className : classNameList) {
            builder.append(sep);
            builder.append(className);
            sep = "|";
        }

        return builder.toString();
    }
}
