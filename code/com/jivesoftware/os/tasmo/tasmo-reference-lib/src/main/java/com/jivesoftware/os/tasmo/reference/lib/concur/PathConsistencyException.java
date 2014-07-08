package com.jivesoftware.os.tasmo.reference.lib.concur;

import com.google.common.collect.Sets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;

public class PathConsistencyException extends RuntimeException {

    private final Set<FieldVersion> wanted;
    private final Set<FieldVersion> got;

    public PathConsistencyException(Set<FieldVersion> wanted, Set<FieldVersion> got) {
        this.wanted = wanted;
        this.got = got;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("PathModifiedOutFromUnderneathMeException{");
        List<FieldVersion> missing = new ArrayList<>(Sets.difference(wanted, got));
        List<FieldVersion> present = new ArrayList<>(Sets.difference(got, wanted));
        Collections.sort(missing, new Comparator<FieldVersion>() {

            @Override
            public int compare(FieldVersion o1, FieldVersion o2) {
                int i = o1.getObjectId().compareTo(o2.getObjectId());
                if (i != 0) {
                    return i;
                }
                i = o1.getFieldName().compareTo(o2.getFieldName());
                if (i != 0) {
                    return i;
                }
                return i;
            }
        });
        sb.append("Missing:");
        for (FieldVersion fieldVersion : missing) {
            sb.append(fieldVersion).append(",");
        }

        Collections.sort(present, new Comparator<FieldVersion>() {

            @Override
            public int compare(FieldVersion o1, FieldVersion o2) {
                int i = o1.getObjectId().compareTo(o2.getObjectId());
                if (i != 0) {
                    return i;
                }
                i = o1.getFieldName().compareTo(o2.getFieldName());
                if (i != 0) {
                    return i;
                }
                return i;
            }
        });

        sb.append("Present:");
        for (FieldVersion fieldVersion : present) {
            sb.append(fieldVersion).append(",");
        }

//        for (int i = 0; i < Math.min(wanted.size(), got.size()); i++) {
//            if (!wanted.get(i).equals(got.get(i))) {
//                if (wanted.get(i).getObjectId().equals(got.get(i).getObjectId())) {
//                    sb.append("(").append(wanted.get(i).getObjectId()).append(" ");
//                } else {
//                    sb.append("(Wanted:").append(wanted.get(i).getObjectId()).append(" but got ").append(got.get(i).getObjectId()).append(" ");
//                }
//                if (wanted.get(i).getFieldName().equals(got.get(i).getFieldName())) {
//                    sb.append(wanted.get(i).getFieldName()).append(" ");
//                } else {
//                    sb.append("Wanted:").append(wanted.get(i).getFieldName()).append(" but got ").append(got.get(i).getFieldName()).append(" ");
//                }
//                if (wanted.get(i).getVersion().equals(got.get(i).getVersion())) {
//                    sb.append(wanted.get(i).getVersion()).append(" ");
//                } else {
//                    sb.append("Wanted:").append(wanted.get(i).getVersion()).append(" but got ").append(got.get(i).getVersion()).append(" ");
//                }
//                sb.append("), ");
//            } else {
//                if (wanted.get(i).getVersion() == got.get(i).getVersion()) {
//                    sb.append("(").append("equal").append("), ");
//                } else {
//                    sb.append("(").append("hmmm").append("), ");
//                }
//            }
//        }
//        for (int i = 1 + Math.min(wanted.size(), got.size()); i < Math.max(wanted.size(), got.size()); i++) {
//            sb.append(wanted.get(i));
//            sb.append(", ");
//        }
        return sb.toString();
    }

}
