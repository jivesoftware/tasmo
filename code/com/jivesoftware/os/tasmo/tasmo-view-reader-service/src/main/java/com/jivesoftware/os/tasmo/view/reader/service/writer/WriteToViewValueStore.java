/*
 * $Revision$
 * $Date$
 *
 * Copyright (C) 1999-$year$ Jive Software. All rights reserved.
 *
 * This software is the proprietary information of Jive Software. Use is subject to license terms.
 */
package com.jivesoftware.os.tasmo.view.reader.service.writer;

import com.jivesoftware.os.tasmo.id.TenantIdAndCentricId;
import com.jivesoftware.os.tasmo.view.reader.service.writer.ViewValueWriter.Transaction;
import java.io.IOException;
import java.util.List;

/**
 *
 */
public class WriteToViewValueStore implements ViewWriter {

    private final ViewValueWriter viewValueStore;

    public WriteToViewValueStore(ViewValueWriter viewValueStore) {
        this.viewValueStore = viewValueStore;
    }

    @Override
    public void write(TenantIdAndCentricId tenantIdAndCentricId, List<ViewWriteFieldChange> fieldChanges) throws ViewWriterException {
        try {
            Transaction transaction = viewValueStore.begin(tenantIdAndCentricId);
            for (ViewWriteFieldChange fieldChange : fieldChanges) {
                if (fieldChange.getType() == ViewWriteFieldChange.Type.add) {
                    transaction.set(fieldChange.getViewObjectId(),
                        fieldChange.getModelPathId(),
                        fieldChange.getModelPathInstanceIds(),
                        fieldChange.getValue(),
                        fieldChange.getTimestamp());
                } else if (fieldChange.getType() == ViewWriteFieldChange.Type.remove) {
                    transaction.remove(fieldChange.getViewObjectId(),
                        fieldChange.getModelPathId(),
                        fieldChange.getModelPathInstanceIds(),
                        fieldChange.getTimestamp());
                } else {
                    throw new ViewWriterException("Unknown change type." + fieldChange.getType());
                }
            }
            viewValueStore.commit(transaction);
        } catch (IOException | ViewWriterException x) {
            throw new ViewWriterException("Failed to write view fields changes for tenantIdAndCentricId:" + tenantIdAndCentricId, x);
        }
    }
}
