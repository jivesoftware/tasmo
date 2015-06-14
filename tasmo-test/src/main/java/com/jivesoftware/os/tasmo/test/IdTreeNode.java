package com.jivesoftware.os.tasmo.test;

import com.jivesoftware.os.jive.utils.id.Id;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

public class IdTreeNode {
    private final List<IdTreeNode> children = new ArrayList<>();
    private final Id value;
    private final IdTreeNode parent;

    IdTreeNode(IdTreeNode parent, Id value) {
        this.parent = parent;
        if (parent != null) {
            parent.children.add(this); // yuk
        }
        this.value = value;
    }

    public List<IdTreeNode> children() {
        return Collections.unmodifiableList(children);
    }

    public Id value() {
        return value;
    }

    public IdTreeNode parent() {
        return parent;
    }

    public void accumulateAtDepth(Set<Id> accumulation, int currentDepth, int targetDepth) {
        if (currentDepth == targetDepth) {
            accumulation.add(value);
        } else {
            for (IdTreeNode child : children) {
                child.accumulateAtDepth(accumulation, currentDepth + 1, targetDepth);
            }
        }
    }

}
