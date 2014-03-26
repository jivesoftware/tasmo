/*
 * Copyright 2014 pete.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.jivesoftware.os.tasmo.view.notification.lib;

import com.google.common.collect.Multimap;
import com.jivesoftware.os.tasmo.id.ChainedVersion;
import com.jivesoftware.os.tasmo.model.ViewBinding;

/**
 *
 * @author pete
 */
public class VersionedNotifiableViewModel {

    private final ChainedVersion version;
    private final Multimap<String, ViewBinding> bindings;

    public VersionedNotifiableViewModel(ChainedVersion version, Multimap<String, ViewBinding> bindings) {
        this.version = version;
        this.bindings = bindings;
    }

    public Multimap<String, ViewBinding> getBindings() {
        return bindings;
    }

    public ChainedVersion getVersion() {
        return version;
    }
}
