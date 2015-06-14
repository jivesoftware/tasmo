/*
 * $Revision$
 * $Date$
 *
 * Copyright (C) 1999-$year$ Jive Software. All rights reserved.
 *
 * This software is the proprietary information of Jive Software. Use is subject to license terms.
 */
package com.jivesoftware.os.tasmo.view.reader.service.writer;

import com.jivesoftware.os.jive.utils.id.TenantIdAndCentricId;
import java.util.List;

/**
 *
 */
public interface ViewWriter {

    void write(TenantIdAndCentricId tenantIdAndCentricId, List<ViewWriteFieldChange> fieldChanges) throws ViewWriterException;
}
