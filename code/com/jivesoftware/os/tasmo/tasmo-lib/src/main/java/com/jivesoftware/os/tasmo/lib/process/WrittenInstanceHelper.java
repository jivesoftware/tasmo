package com.jivesoftware.os.tasmo.lib.process;

import com.jivesoftware.os.tasmo.id.ObjectId;
import com.jivesoftware.os.tasmo.model.process.WrittenInstance;
import com.jivesoftware.os.tasmo.reference.lib.Reference;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class WrittenInstanceHelper {

    public Collection<Reference> getReferencesFromInstanceField(WrittenInstance writtenInstance, String referenceFieldName) {
        ObjectId value = writtenInstance.getReferenceFieldValue(referenceFieldName);
        if (value != null) {
            return Arrays.asList(new Reference(value, referenceFieldName));
        } else {
            ObjectId[] values = writtenInstance.getMultiReferenceFieldValue(referenceFieldName);
            if (values == null) {
                return Collections.<Reference>emptyList();
            } else {
                List<Reference> references = new ArrayList<>(values.length);
                for (ObjectId v : values) {
                    references.add(new Reference(v, referenceFieldName));
                }
                return references;
            }
        }
    }

}
