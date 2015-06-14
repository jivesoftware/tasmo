/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.jivesoftware.os.tasmo.model.path;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Allows lookup from view path id to the set of classes along the path
 *
 */
public class ViewPathDictionary implements ViewPathKeyProvider {

    private final Map<Long, String[]> dictionary = new HashMap<>();
    private final ViewPathKeyProvider viewPathKeyProvider;

    public ViewPathDictionary(ModelPath path, ViewPathKeyProvider viewPathKeyProvider) {
        this.viewPathKeyProvider = viewPathKeyProvider;

        for (String[] combination : computeAllCombinationsForPath(path.getPathMembers())) {
            long key = pathKeyHashcode(combination);
            if (dictionary.containsKey(key)) {
                String[] existingCombination = dictionary.get(key);
                throw new IllegalStateException("Hash collision detected between these two view paths: "
                        + Arrays.toString(existingCombination) + " and " + Arrays.toString(combination));
            }

            dictionary.put(key, combination);
        }
    }

    public String[] lookupModelPathClasses(long pathKey) {
        return dictionary.get(pathKey);
    }

    @Override
    public long pathKeyHashcode(String[] classes) {
        return viewPathKeyProvider.pathKeyHashcode(classes);
    }

    @Override
    public long modelPathHashcode(String modelPathId) {
        return viewPathKeyProvider.modelPathHashcode(modelPathId);
    }

    private Iterable<String[]> computeAllCombinationsForPath(List<ModelPathStep> pathMembers) {
        Odometer[] odometers = new Odometer[pathMembers.size()];

        for (int i = 0; i < pathMembers.size(); i++) {
            ModelPathStep step = pathMembers.get(i);
            Odometer stepOdometer;
            if (step.getStepType().isBackReferenceType()) {
                Set<String> stepClasses = step.getDestinationClassNames();
                stepOdometer = new Odometer(stepClasses.toArray(new String[stepClasses.size()]));
            } else {
                Set<String> stepClasses = step.getOriginClassNames();
                stepOdometer = new Odometer(stepClasses.toArray(new String[stepClasses.size()]));
            }

            odometers[odometers.length - 1 - i] = stepOdometer;
        }

        Odometer last = null;
        for (Odometer step : odometers) {
            if (last != null) {
                last.setNext(step);
            }
            last = step;
        }

        Odometer head = odometers[0];
        List<String[]> allCombinations = new ArrayList<>();
        allCombinations.add(convert(head.toArray()));

        while (head.inc()) {
            allCombinations.add(convert(head.toArray()));
        }

        return allCombinations;
    }

    private String[] convert(Object[] objectArray) {
        return Arrays.copyOf(objectArray, objectArray.length, String[].class);
    }

    static public class Odometer {

        /**
         *
         */
        public int roleOver = 0;
        private int index = 0;
        private Object[] values;
        private Odometer next;

        /**
         *
         * @param _values
         */
        public Odometer(Object[] _values) {
            values = Arrays.copyOf(_values, _values.length);
        }

        /**
         *
         * @param _index
         */
        public void setIndex(int _index) {
            if (_index < 0) {
                _index = 0;
            }
            if (_index >= values.length) {
                _index = values.length - 1;
            }
            index = _index;
        }

        /**
         *
         * @param _odometer
         * @return
         */
        public Odometer setNext(Odometer _odometer) {
            next = _odometer;
            return next;
        }

        /**
         *
         * @return
         */
        public Odometer getNext() {
            return next;
        }

        /**
         *
         * @return
         */
        public boolean inc() {
            index++;
            if (index >= values.length) {
                roleOver++;
                index = 0;
                if (next == null) {
                    return false;
                }
                return next.inc();
            }
            return true;
        }

        /**
         *
         * @return
         */
        public Object[] toArray() {
            if (next == null) {
                return new Object[]{values[index]};
            } else {
                return push(next.toArray(), values[index]);
            }
        }

        private Object[] push(Object[] src, Object instance) {
            if (src == null) {
                src = new Object[0];
            }
            Object[] newSrc = new Object[src.length + 1];
            System.arraycopy(src, 0, newSrc, 0, src.length);
            newSrc[src.length] = instance;
            return newSrc;
        }
    }
}
