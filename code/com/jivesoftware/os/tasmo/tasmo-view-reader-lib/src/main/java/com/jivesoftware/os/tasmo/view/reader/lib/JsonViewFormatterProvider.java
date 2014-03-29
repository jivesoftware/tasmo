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
package com.jivesoftware.os.tasmo.view.reader.lib;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.jivesoftware.os.tasmo.model.process.WrittenEventProvider;

/**
 *
 * @author pete
 */
public class JsonViewFormatterProvider implements ViewFormatterProvider<ObjectNode> {

    private final ObjectMapper mapper;
    private final WrittenEventProvider<ObjectNode, JsonNode> writtenEventProvider;

    public JsonViewFormatterProvider(ObjectMapper mapper, WrittenEventProvider<ObjectNode, JsonNode> writtenEventProvider) {
        this.mapper = mapper;
        this.writtenEventProvider = writtenEventProvider;
    }

    @Override
    public ViewFormatter<ObjectNode> createViewFormatter() {
        return new JsonViewFormatter(mapper, writtenEventProvider);
    }
}
