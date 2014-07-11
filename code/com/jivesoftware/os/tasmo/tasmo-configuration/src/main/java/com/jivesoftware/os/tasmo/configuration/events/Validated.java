package com.jivesoftware.os.tasmo.configuration.events;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.jivesoftware.os.jive.utils.id.ChainedVersion;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 *
 */
public class Validated {

    private final ChainedVersion version;
    private final Collection<String> infos;
    private final Collection<String> errors;

    private Validated(ChainedVersion version, List<String> infos, List<String> errors) {
        this.version = version;

        this.infos = infos;
        this.errors = errors;
    }

    public ChainedVersion getVersion() {
        return version;
    }

    public boolean isValid() {
        return errors.isEmpty();
    }

    public Collection<String> getInfoMessages() {
        return infos;
    }

    public Collection<String> getErrorMessages() {
        return errors;
    }

    @Override
    public String toString() {
        return "Validated{"
            + "version=" + version
            + ", valid=" + isValid()
            + ", infos=[" + Joiner.on(',').join(infos)
            + "], errors=[" + Joiner.on(',').join(errors)
            + "]}";
    }

    public static ValidatedBuilder build() {
        return new ValidatedBuilder();
    }

    public static class ValidatedBuilder {
        private ChainedVersion version;
        private ArrayList<String> infos;
        private ArrayList<String> errors;

        public ValidatedBuilder setVersion(ChainedVersion version) {
            this.version = version;
            return this;
        }

        public ValidatedBuilder addMessage(boolean error, String message) {
            Preconditions.checkNotNull(message);
            if (error) {
                addError(message);
            } else {
                addInfo(message);
            }
            return this;
        }

        public ValidatedBuilder addInfo(String message) {
            Preconditions.checkNotNull(message);
            if (infos == null) {
                infos = new ArrayList<>();
            }
            infos.add(message);
            return this;
        }

        public ValidatedBuilder addError(String message) {
            Preconditions.checkNotNull(message);
            if (errors == null) {
                errors = new ArrayList<>();
            }
            errors.add(message);
            return this;
        }

        public Validated build() {
            List<String> infoList = (this.infos == null) ? Collections.<String>emptyList() : this.infos;
            List<String> errorList = (this.errors == null) ? Collections.<String>emptyList() : this.errors;
            return new Validated(version, infoList, errorList);
        }

    }
}
