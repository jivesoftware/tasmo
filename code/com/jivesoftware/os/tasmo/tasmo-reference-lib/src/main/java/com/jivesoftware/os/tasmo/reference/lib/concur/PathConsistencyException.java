package com.jivesoftware.os.tasmo.reference.lib.concur;

import com.jivesoftware.os.tasmo.reference.lib.concur.ConcurrencyStore.FieldVersion;
import java.util.List;

public class PathConsistencyException extends RuntimeException {

    private final List<FieldVersion> wanted;
    private final List<FieldVersion> got;

    public PathConsistencyException(List<FieldVersion> wanted, List<FieldVersion> got) {
        this.wanted = wanted;
        this.got = got;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("PathModifiedOutFromUnderneathMeException{");
        for (int i = 0; i < Math.min(wanted.size(), got.size()); i++) {
            if (!wanted.get(i).equals(got.get(i))) {
                if (wanted.get(i).getObjectId().equals(got.get(i).getObjectId())) {
                    sb.append("(").append(wanted.get(i).getObjectId()).append(" ");
                } else {
                    sb.append("(Wanted:").append(wanted.get(i).getObjectId()).append(" but got ").append(got.get(i).getObjectId()).append(" ");
                }
                if (wanted.get(i).getFieldName().equals(got.get(i).getFieldName())) {
                    sb.append(wanted.get(i).getFieldName()).append(" ");
                } else {
                    sb.append("Wanted:").append(wanted.get(i).getFieldName()).append(" but got ").append(got.get(i).getFieldName()).append(" ");
                }
                if (wanted.get(i).getVersion().equals(got.get(i).getVersion())) {
                    sb.append(wanted.get(i).getVersion()).append(" ");
                } else {
                    sb.append("Wanted:").append(wanted.get(i).getVersion()).append(" but got ").append(got.get(i).getVersion()).append(" ");
                }
                sb.append("), ");
            } else {
                if (wanted.get(i).getVersion() == got.get(i).getVersion()) {
                    sb.append("(").append("equal").append("), ");
                } else {
                    sb.append("(").append("hmmm").append("), ");
                }
            }
        }
        for (int i = 1 + Math.min(wanted.size(), got.size()); i < Math.max(wanted.size(), got.size()); i++) {
            sb.append(wanted.get(i));
            sb.append(", ");
        }
        return sb.toString();
    }

}
