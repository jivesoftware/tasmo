package com.jivesoftware.os.tasmo.lib.modifier;

import com.jivesoftware.os.jive.utils.id.Id;
import com.jivesoftware.os.jive.utils.id.TenantId;
import com.jivesoftware.os.jive.utils.row.column.value.store.api.RowColumnValueStore;
import com.jivesoftware.os.jive.utils.row.column.value.store.api.timestamper.ConstantTimestamper;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

/**
 *
 */
public class ModifierStore {

    private final RowColumnValueStore<TenantId, Id, Id, String, RuntimeException> modifierStore;

    public ModifierStore(RowColumnValueStore<TenantId, Id, Id, String, RuntimeException> modifierStore) {
        this.modifierStore = modifierStore;
    }

    public List<Id> get(TenantId tenantId, Id userId, Id[] ids) {
        List<Id> had = new ArrayList<>();
        List<String> got = modifierStore.multiGet(tenantId, userId, ids, null, null);
        for (int i = 0; i < ids.length; i++) {
            if (got.get(i) != null) {
                had.add(ids[i]);
            }
        }
        return had;
    }

    public void add(TenantId tenantId, Id userId, Set<Id> ids, long timestamp) {
        String[] values = new String[ids.size()];
        Arrays.fill(values, "");
        modifierStore.multiAdd(tenantId, userId, ids.toArray(new Id[ids.size()]), values, Integer.MIN_VALUE, null);
    }

    public void remove(TenantId tenantId, Id userId, Id[] ids, long removeAtTimestamp) {

        modifierStore.multiRemove(tenantId, userId, ids, new ConstantTimestamper(removeAtTimestamp));
    }

}
