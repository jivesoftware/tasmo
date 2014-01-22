/*
 * $Revision$
 * $Date$
 *
 * Copyright (C) 1999-$year$ Jive Software. All rights reserved.
 *
 * This software is the proprietary information of Jive Software. Use is subject to license terms.
 */
package com.jivesoftware.os.tasmo.view.reader.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.jivesoftware.os.tasmo.id.Id;

/**
 *
 */
interface Collector {

    void process(Collector[] collectors, int i, Id orderId, String value, long timestamp) throws Exception;

    void link(JsonNode value, long timestamp);

    ObjectNode active();
}
