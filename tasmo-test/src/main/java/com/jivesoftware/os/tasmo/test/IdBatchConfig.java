package com.jivesoftware.os.tasmo.test;

/**
 *
 * @author jonathan.colt
 */
public class IdBatchConfig {
    final Order order;
    final int numberOfIdsInBatch;
    final int numberOfBatches;

    public IdBatchConfig(Order order, int numberOfIdsInBatch, int numberOfBatches) {
        this.order = order;
        this.numberOfIdsInBatch = numberOfIdsInBatch;
        this.numberOfBatches = numberOfBatches;
    }

    @Override
    public String toString() {
        return order + " " + numberOfIdsInBatch + " " + numberOfBatches;
    }

}
