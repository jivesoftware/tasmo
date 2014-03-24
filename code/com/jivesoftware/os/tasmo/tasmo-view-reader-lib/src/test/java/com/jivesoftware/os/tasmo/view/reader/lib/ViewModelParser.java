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
package com.jivesoftware.os.tasmo.view.reader.lib;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.jivesoftware.os.tasmo.id.ChainedVersion;
import com.jivesoftware.os.tasmo.id.TenantId;
import com.jivesoftware.os.tasmo.model.ViewBinding;
import com.jivesoftware.os.tasmo.model.Views;
import com.jivesoftware.os.tasmo.model.path.ModelPath;
import com.jivesoftware.os.tasmo.model.path.ModelPathStep;
import com.jivesoftware.os.tasmo.model.path.ModelPathStepType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

/**
 *
 * @author pete
 */
public class ViewModelParser {

    private final TenantId tenantId;
    private final ChainedVersion version;

    public ViewModelParser(TenantId tenantId, ChainedVersion version) {
        this.tenantId = tenantId;
        this.version = version;
    }

    public Views parse(String viewModel) {
        return parse(false, Arrays.asList(viewModel));
    }

    public Views parse(boolean idCentric, String viewModel) {
        return parse(idCentric, Arrays.asList(viewModel));
    }

    public Views parse(List<String> viewModel) {
        return parse(false, viewModel);
    }

    public Views parse(boolean idCentric, List<String> viewModel) {
        List<ViewBinding> bindings = parseModelPathStrings(idCentric, viewModel);
        return new Views(tenantId, version, bindings);
    }

    List<ViewBinding> parseModelPathStrings(boolean idCentric, List<String> simpleBindings) {
        ArrayListMultimap<String, ModelPath> viewBindings = ArrayListMultimap.create();

        for (String simpleBinding : simpleBindings) {
            String[] class_pathId_modelPath = toStringArray(simpleBinding, "::");
            List<ModelPath> bindings = viewBindings.get(class_pathId_modelPath[0].trim());

            bindings.add(buildPath(class_pathId_modelPath[1].trim(), class_pathId_modelPath[2].trim()));
        }

        List<ViewBinding> viewBindingsList = Lists.newArrayList();
        for (Map.Entry<String, Collection<ModelPath>> entry : viewBindings.asMap().entrySet()) {
            viewBindingsList.add(new ViewBinding(entry.getKey(), new ArrayList<>(entry.getValue()), false, idCentric, false, null));
        }

        return viewBindingsList;
    }

    private ModelPath buildPath(String id, String path) {
        String[] pathMembers = toStringArray(path, "|");
        ModelPath.Builder builder = ModelPath.builder(id);
        int i = 0;
        for (String pathMember : pathMembers) {
            builder.addPathMember(toModelPathMember(i, pathMember.trim()));
            i++;
        }
        return builder.build();
    }

    private ModelPathStep toModelPathMember(int sortPrecedence, String pathMember) {

        try {
            String[] memberParts = toStringArray(pathMember, ".");
            if (pathMember.contains("." + ModelPathStepType.ref + ".") || pathMember.contains("." + ModelPathStepType.refs + ".")) {
                // Example: Content.ref_originalAuthor.ref.User
                Set<String> originClassName = splitClassNames(memberParts[0].trim());
                String refFieldName = memberParts[1].trim();
                ModelPathStepType stepType = ModelPathStepType.valueOf(memberParts[2].trim());
                Set<String> destinationClassName = splitClassNames(memberParts[3].trim());

                return new ModelPathStep(sortPrecedence == 0, originClassName,
                    refFieldName, stepType, destinationClassName, null);

            } else if (pathMember.contains("." + ModelPathStepType.backRefs + ".")
                || pathMember.contains("." + ModelPathStepType.count + ".")
                || pathMember.contains("." + ModelPathStepType.latest_backRef + ".")) {

                // Example: Content.backRefs.VersionedContent.ref_parent
                // Example: Content.count.VersionedContent.ref_parent
                // Example: Content.latest_backRef.VersionedContent.ref_parent
                Set<String> destinationClassName = splitClassNames(memberParts[0].trim());
                ModelPathStepType stepType = ModelPathStepType.valueOf(memberParts[1].trim());
                Set<String> originClassName = splitClassNames(memberParts[2].trim());
                String refFieldName = memberParts[3].trim();

                return new ModelPathStep(sortPrecedence == 0, originClassName,
                    refFieldName, stepType, destinationClassName, null);

            } else {

                // Example: User.firstName
                String[] valueFieldNames = toStringArray(memberParts[1], ",");
                for (int i = 0; i < valueFieldNames.length; i++) {
                    valueFieldNames[i] = valueFieldNames[i].trim();
                }
                Set<String> originClassName = splitClassNames(memberParts[0].trim());

                return new ModelPathStep(sortPrecedence == 0, originClassName,
                    null, ModelPathStepType.value, null, Arrays.asList(valueFieldNames));

            }
        } catch (Exception x) {
            throw new RuntimeException("fail to parse " + pathMember, x);
        }
    }

    private Set<String> splitClassNames(String classNames) {
        if (classNames.startsWith("[")) {
            classNames = classNames.replace("[", "");
            classNames = classNames.replace("]", "");

            return Sets.newHashSet(classNames.split("\\^"));
        } else {
            return Sets.newHashSet(classNames);
        }
    }

    private String[] toStringArray(String string, String delim) {
        if (string == null || delim == null) {
            return new String[0];
        }
        StringTokenizer tokenizer = new StringTokenizer(string, delim);
        int tokenCount = tokenizer.countTokens();

        String[] tokens = new String[tokenCount];
        for (int i = 0; i < tokenCount; i++) {
            tokens[i] = tokenizer.nextToken();
        }
        return tokens;
    }
}
