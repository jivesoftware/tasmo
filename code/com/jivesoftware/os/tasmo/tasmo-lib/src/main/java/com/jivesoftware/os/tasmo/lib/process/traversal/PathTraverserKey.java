package com.jivesoftware.os.tasmo.lib.process.traversal;

import java.util.Objects;
import java.util.Set;

/**
 *
 * @author jonathan
 */
public class PathTraverserKey {

    private final Set<String> initialFieldNames;
    private final int pathIndex;
    private final int pathLength;

    public PathTraverserKey(Set<String> initialFieldNames, int pathIndex, int pathLength) {
        this.initialFieldNames = initialFieldNames;
        this.pathIndex = pathIndex;
        this.pathLength = pathLength;
    }

    public Set<String> getInitialFieldNames() {
        return initialFieldNames;
    }

    public int getPathIndex() {
        return pathIndex;
    }

    public int getPathLength() {
        return pathLength;
    }

    @Override
    public String toString() {
        return "PathTraverserKey{" + "initialFieldNames=" + initialFieldNames + ", pathIndex=" + pathIndex + ", pathLength=" + pathLength + '}';
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 47 * hash + Objects.hashCode(this.initialFieldNames);
        hash = 47 * hash + this.pathIndex;
        hash = 47 * hash + this.pathLength;
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final PathTraverserKey other = (PathTraverserKey) obj;
        if (!Objects.equals(this.initialFieldNames, other.initialFieldNames)) {
            return false;
        }
        if (this.pathIndex != other.pathIndex) {
            return false;
        }
        if (this.pathLength != other.pathLength) {
            return false;
        }
        return true;
    }

}
