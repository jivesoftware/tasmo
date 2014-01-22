/*
 * $Revision$
 * $Date$
 *
 * Copyright (C) 1999-$year$ Jive Software. All rights reserved.
 *
 * This software is the proprietary information of Jive Software. Use is subject to license terms.
 */
package com.jivesoftware.os.tasmo.test;

import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import org.paukov.combinatorics.Factory;
import org.paukov.combinatorics.Generator;
import org.paukov.combinatorics.ICombinatoricsVector;

/**
 *
 * @author jonathan.colt
 */
public class OrderIdProviderGenerator {

    public List<OrderIdProvider> generateOrderIdProviders(long seed, long initial, IdBatchConfig... idBatchConfigs) {

        final Random random = new Random(seed);

        List<List<List<Long>>> permutateBatchs = new ArrayList<>();

        int start = 0;
        for (IdBatchConfig idBatchConfig : idBatchConfigs) {

            List<Long> ids = new ArrayList<>();
            for (int i = 0; i < idBatchConfig.numberOfIdsInBatch; i++, start++) {
                ids.add(initial + ((long) start * 2));
            }

            List<List<Long>> batchPermutations = new ArrayList<>();
            if (idBatchConfig.order == Order.montomically) {
                batchPermutations.add(new ArrayList<>(ids));
            } else if (idBatchConfig.order == Order.shuffle) {
                for (int s = 0; s < idBatchConfig.numberOfBatches; s++) {
                    Collections.shuffle(ids, random);
                    batchPermutations.add(new ArrayList<>(ids));
                }
            } else if (idBatchConfig.order == Order.permutations) {
                if (ids.size() == 1) {
                    batchPermutations.add(new ArrayList<>(ids));
                } else {
                    ICombinatoricsVector<Long> originalVector = Factory.createVector(ids);
                    Generator<Long> generator = Factory.createPermutationGenerator(originalVector);
                    for (ICombinatoricsVector<Long> permutation : generator) {
                        //println(Arrays.deepToString(permutation.getVector().toArray()));
                        batchPermutations.add(new ArrayList<>(permutation.getVector()));
                    }
                }
            }
            permutateBatchs.add(batchPermutations);
        }

        Odometer head = new Odometer(permutateBatchs.get(permutateBatchs.size() - 1).toArray());
        Odometer chain = head;
        for (int i = permutateBatchs.size() - 1; i > -1; i--) {
            Odometer odometer = new Odometer(permutateBatchs.get(i).toArray());
            chain.setNext(odometer);
            chain = odometer;
        }

        List<List<Long>> permutations = new ArrayList<>();
        Object[] toArray = head.toArray();
        List<Long> join = new ArrayList<>();
        for (Object a : toArray) {
            join.addAll((List<Long>) a);
        }
        //System.out.println("r:" + join + " " + idBatchConfigs.length);
        permutations.add(join);
        while (head.inc()) {
            toArray = head.toArray();
            join = new ArrayList<>();
            for (Object a : toArray) {
                join.addAll((List<Long>) a);
            }
            //System.out.println("r:" + join + " " + idBatchConfigs.length);
            permutations.add(join);
        }
        //System.out.println(Arrays.deepToString(idBatchConfigs) + ":" + permutations.size());

        List<OrderIdProvider> providers = new ArrayList<>(permutations.size());
        for (List<Long> permutation : permutations) {
            final List<Long> idBatch = permutation;
            providers.add(new OrderIdProvider() {
                @Override
                public long nextId() {
                    return idBatch.remove(0);
                }
            });
        }

        return providers;
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

        public Odometer setNext(Odometer _odometer) {
            next = _odometer;
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
                return new Object[]{ values[index] };
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
