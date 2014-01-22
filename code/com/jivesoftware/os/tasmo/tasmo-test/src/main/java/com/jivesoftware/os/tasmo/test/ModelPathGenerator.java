/*
 * $Revision$
 * $Date$
 *
 * Copyright (C) 1999-$year$ Jive Software. All rights reserved.
 *
 * This software is the proprietary information of Jive Software. Use is subject to license terms.
 */
package com.jivesoftware.os.tasmo.test;

import com.jivesoftware.os.tasmo.model.path.ModelPathStepType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Generates model path strings (the internal config represenation of a model path). For a given set of reference types it will generate all possible
 * combinations.
 *
 */
public class ModelPathGenerator {

    private final int alphabetZero = 65;

    public List<String> generateModelPaths(List<ModelPathStepType> refTypes, int maxNumSteps) {
        if (maxNumSteps == 0) {
            return Collections.emptyList(); //if only everything were this easy
        }

        if (refTypes.contains(ModelPathStepType.value)) {
            refTypes = new ArrayList<>(refTypes);
            refTypes.remove(ModelPathStepType.value);
        }
        List<String> paths = new ArrayList<>();

        //path length of 1 - no references
        paths.add(generatePathFromStepTypes(Collections.<ModelPathStepType>emptyList(), "RootValues"));

        for (int i = 1; i < maxNumSteps; i++) {
            paths.addAll(generateAllPathCombinations(refTypes, i));
        }
        return paths;

    }

    public static void main(String[] args) {

        ModelPathGenerator testGenerator = new ModelPathGenerator();

        testGenerator.generateModelPaths(Arrays.asList(ModelPathStepType.values()), 4);
    }

    private List<String> generateAllPathCombinations(List<ModelPathStepType> refTypes, int numRefHops) {
        int[] indices = new int[numRefHops];
        int numTypes = refTypes.size();

        List<String> paths = new ArrayList<>((int) Math.pow(numTypes, numRefHops));

        for (int i = 1; i <= Math.pow(numTypes, numRefHops); i++) {

            List<ModelPathStepType> combination = new ArrayList<>(numRefHops);
            for (int idx : indices) {
                ModelPathStepType type = refTypes.get(idx);
                combination.add(type);
            }

            paths.add(generatePathFromStepTypes(combination, "PathId" + (int) (Math.random() * Integer.MAX_VALUE))); //bleh - need to pass in a generator here

            int idxVal = indices[0];
            indices[0] = (idxVal + 1) % numTypes;

            for (int j = 1; j < indices.length; j++) {
                if (i % Math.pow(numTypes, j) == 0) {
                    idxVal = indices[j];
                    indices[j] = (idxVal + 1) % numTypes;
                }
            }
        }

        return paths;
    }

    private String generatePathFromStepTypes(List<ModelPathStepType> stepTypes, String pathId) {
        StringBuilder builder = new StringBuilder();
        builder.append("ViewClass::");
        builder.append(pathId);
        builder.append("::");

        //forward ref
        //ContentView + "::" + moderatorNames + "::Content.ref_parent.ref.Container|Container.refs_moderators.refs.User|User.userName"

        //back ref
        //viewClassName + "::" + viewFieldName + "::Content.backRefs.User.ref_content|User.userName,age"
        int i = 0;
        for (; i < stepTypes.size(); i++) {
            ModelPathStepType type = stepTypes.get(i);

            builder.append((char) (i + alphabetZero)).append(".");
            if (type.isBackReferenceType()) {
                builder.append(type.name()).append(".");
                builder.append((char) (i + 1 + alphabetZero)).append(".");
                builder.append("referringField");
            } else {
                builder.append("referringfield");
                builder.append(".").append(type.name());
                builder.append(".").append((char) (i + 1 + alphabetZero));
            }

            builder.append("|");
        }

        builder.append((char) (i + alphabetZero)).append(".value1,value2,value3");


//        System.out.println(builder.toString());
        return builder.toString();

    }
}
