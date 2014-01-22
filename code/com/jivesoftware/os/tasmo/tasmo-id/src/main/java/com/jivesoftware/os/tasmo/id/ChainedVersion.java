/*
 * $Revision$
 * $Date$
 *
 * Copyright (C) 1999-$year$ Jive Software. All rights reserved.
 *
 * This software is the proprietary information of Jive Software. Use is subject to license terms.
 */
package com.jivesoftware.os.tasmo.id;

import java.util.Objects;

/**
 *
 * @author jonathan.colt
 */
public class ChainedVersion {

    public static ChainedVersion fromStringForm(String stringForm) {
        if (stringForm.length() == 1 && stringForm.equals(VERSION_SEPERATOR)) {
            return NULL;
        }
        return new ChainedVersion(stringForm);
    }
    public static final ChainedVersion NULL = new ChainedVersion("", "");
    private static final String VERSION_SEPERATOR = ":";
    private final String priorVersion;
    private final String version;

    public ChainedVersion(String stringForm) {

        String[] priorVersion_version = stringForm.split(VERSION_SEPERATOR);
        if (priorVersion_version.length != 2) {
            throw new IllegalArgumentException("version=" + stringForm + " is not of the form 'priorVersion:currentVersion'");
        }
        this.priorVersion = priorVersion_version[0];
        this.version = priorVersion_version[1];
    }

    public ChainedVersion(String priorVersion, String version) {
        this.priorVersion = priorVersion;
        this.version = version;
    }

    public String getPriorVersion() {
        return priorVersion;
    }

    public String getVersion() {
        return version;
    }

    public String toStringForm() {
        return priorVersion + VERSION_SEPERATOR + version;
    }

    @Override
    public String toString() {
        return "Version{" + "priorVersion=" + priorVersion + ", version=" + version + '}';
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 47 * hash + Objects.hashCode(this.priorVersion);
        hash = 47 * hash + Objects.hashCode(this.version);
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
        final ChainedVersion other = (ChainedVersion) obj;
        if (!Objects.equals(this.priorVersion, other.priorVersion)) {
            return false;
        }
        if (!Objects.equals(this.version, other.version)) {
            return false;
        }
        return true;
    }
}
