package com.jivesoftware.os.tasmo.lib.modifier;

import com.jivesoftware.os.jive.utils.id.Id;
import com.jivesoftware.os.jive.utils.id.ObjectId;
import com.jivesoftware.os.jive.utils.id.TenantId;
import com.jivesoftware.os.rcvs.api.ColumnValueAndTimestamp;
import com.jivesoftware.os.rcvs.api.RowColumnValueStore;
import com.jivesoftware.os.rcvs.api.timestamper.ConstantTimestamper;
import com.jivesoftware.os.tasmo.lib.write.PathId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

/**
 *
 */
public class ModifierStore {

    private final RowColumnValueStore<TenantId, Id, ObjectId, String, RuntimeException> modifierStore;

    public ModifierStore(RowColumnValueStore<TenantId, Id, ObjectId, String, RuntimeException> modifierStore) {
        this.modifierStore = modifierStore;
    }

    public List<PathId> get(TenantId tenantId, Id userId, Set<ObjectId> ids) {
        List<PathId> had = new ArrayList<>();
        ObjectId[] objectIds = ids.toArray(new ObjectId[ids.size()]);
        ColumnValueAndTimestamp<ObjectId, String, Long>[] got = modifierStore.multiGetEntries(tenantId, userId, objectIds, null, null);
        for (int i = 0; i < objectIds.length; i++) {
            if (got[i] != null) {
                had.add(new PathId(got[i].getColumn(), got[i].getTimestamp()));
            }
        }
        return had;
    }

    public void add(TenantId tenantId, Id userId, Set<ObjectId> ids, long timestamp) {
        String[] values = new String[ids.size()];
        Arrays.fill(values, "");
        modifierStore.multiAdd(tenantId, userId, ids.toArray(new ObjectId[ids.size()]), values, Integer.MIN_VALUE, null);
    }

    public void remove(TenantId tenantId, Id userId, ObjectId[] ids, long removeAtTimestamp) {
        modifierStore.multiRemove(tenantId, userId, ids, new ConstantTimestamper(removeAtTimestamp));
    }

}
