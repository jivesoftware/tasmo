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

import com.jivesoftware.os.tasmo.id.ObjectId;
import java.util.List;

/**
 *
 * @author pete
 */
public interface ViewFormatter<V> {
    
    V getView();
    
    void addReferenceNode(ViewReference reference, List<ObjectId> presentDestinations);
    
    void addValueNode(ViewValue value);

    void setRoot(ObjectId viewRoot);
    
    void nextLevel();

    void nextPath();
    
}
