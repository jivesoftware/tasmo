package com.jivesoftware.os.tasmo.id;

import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;

/**
 * An order id provider which generates ids using a combination of system time, a logical writer id, and an incrementing sequence number.
 */
public final /* hi mark */ class IdProviderImpl implements IdProvider {

    private final OrderIdProvider orderIdProvider;

    public IdProviderImpl(OrderIdProvider orderIdProvider) {
        this.orderIdProvider = orderIdProvider;
    }

    @Override
    public Id nextId() {
        return new Id(orderIdProvider.nextId());
    }
}
