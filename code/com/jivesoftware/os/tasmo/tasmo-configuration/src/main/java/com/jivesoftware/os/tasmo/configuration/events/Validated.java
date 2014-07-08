package com.jivesoftware.os.tasmo.configuration.events;

import com.jivesoftware.os.jive.utils.id.ChainedVersion;
import java.util.Collection;
import java.util.Collections;
import java.util.Objects;

/**
 *
 */
public class Validated {

    private final ChainedVersion version;
    private final boolean valid;
    private final Collection<String> unexpectedFields;

    private Validated(ChainedVersion version, boolean valid, Collection<String> unexpectedFields) {
        this.version = version;
        this.valid = valid;

        if (unexpectedFields == null) {
            unexpectedFields = Collections.emptyList();
        }
        this.unexpectedFields = unexpectedFields;
    }

    public ChainedVersion getVersion() {
        return version;
    }

    public boolean isValid() {
        return valid;
    }

    public Collection<String> getUnexpectedFields() {
        return Collections.unmodifiableCollection(unexpectedFields);
    }

    @Override
    public String toString() {
        return "Validated{"
            + "version=" + version
            + ", valid=" + valid
            + ", unexpectedFields=" + unexpectedFields
            + '}';
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 53 * hash + Objects.hashCode(this.version);
        hash = 53 * hash + (this.valid ? 1 : 0);
        hash = 53 * hash + Objects.hashCode(this.unexpectedFields);
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
        final Validated other = (Validated) obj;
        if (!Objects.equals(this.version, other.version)) {
            return false;
        }
        if (this.valid != other.valid) {
            return false;
        }
        if (!Objects.equals(this.unexpectedFields, other.unexpectedFields)) {
            return false;
        }
        return true;
    }

    public static ValidatedBuilder build() {
        return new ValidatedBuilder();
    }

    public static class ValidatedBuilder {

        private ChainedVersion version = null;
        private Collection<String> unexpectedFields = Collections.emptyList();

        public ValidatedBuilder setVersion(ChainedVersion version) {
            this.version = version;
            return this;
        }

        public ValidatedBuilder setUnexpectedFields(Collection<String> unexpectedFields) {
            this.unexpectedFields = unexpectedFields;
            return this;
        }

        public Validated invalid() {
            return new Validated(version, false, unexpectedFields);
        }

        public Validated valid() {
            return new Validated(version, true, unexpectedFields);
        }
    }
}
