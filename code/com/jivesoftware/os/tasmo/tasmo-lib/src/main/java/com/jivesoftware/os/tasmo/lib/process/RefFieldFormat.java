/*
 * $Revision$
 * $Date$
 *
 * Copyright (C) 1999-$year$ Jive Software. All rights reserved.
 *
 * This software is the proprietary information of Jive Software. Use is subject to license terms.
 */
package com.jivesoftware.os.tasmo.lib.process;

import com.fasterxml.jackson.databind.JsonNode;
import com.jivesoftware.os.tasmo.id.ObjectId;
import java.util.Collection;

public interface RefFieldFormat {

    Collection<ObjectId> fieldValueToObjectIds(JsonNode jsonNode);
}
