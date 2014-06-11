package com.jivesoftware.os.tasmo.configuration;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.jivesoftware.os.tasmo.event.api.ReservedFields;
import com.jivesoftware.os.tasmo.id.ObjectId;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;

public class ViewObject {

    private final Set<String> classNames;
    private final Set<String> valueFields;
    private final Map<String, ViewArray> arrayFields;
    private final Map<String, ViewObject> refFields;
    private final Map<String, ViewArray> backRefFields;
    private final Map<String, ViewArray> countFields;
    private final Map<String, ViewObject> latestBackRefFields;

    public ViewObject(Set<ObjectId> ids,
        Set<String> valueFields,
        Map<String, ViewArray> arrayFields,
        Map<String, ViewObject> refFields,
        Map<String, ViewArray> backRefFields,
        Map<String, ViewArray> countFields,
        Map<String, ViewObject> latestBackRefFields) {

        this.classNames = new HashSet<String>(ids.size());
        for (ObjectId objectId : ids) {
            classNames.add(objectId.getClassName());
        }

        this.valueFields = valueFields;
        this.arrayFields = arrayFields;
        this.refFields = refFields;
        this.backRefFields = backRefFields;
        this.countFields = countFields;
        this.latestBackRefFields = latestBackRefFields;
    }

    public Set<String> getTriggeringEventClassNames() {
        Set<String> triggeringEventClassNames = new HashSet<>();
        for (String className : classNames) {
            triggeringEventClassNames.add(className);
        }
        for (ViewArray viewArray : arrayFields.values()) {
            triggeringEventClassNames.addAll(viewArray.getTriggeringEventClassNames());
        }
        for (ViewObject viewObject : refFields.values()) {
            triggeringEventClassNames.addAll(viewObject.getTriggeringEventClassNames());
        }
        for (ViewArray viewArray : backRefFields.values()) {
            triggeringEventClassNames.addAll(viewArray.getTriggeringEventClassNames());
        }
        for (ViewArray viewArray : countFields.values()) {
            triggeringEventClassNames.addAll(viewArray.getTriggeringEventClassNames());
        }
        for (ViewObject viewObject : latestBackRefFields.values()) {
            triggeringEventClassNames.addAll(viewObject.getTriggeringEventClassNames());
        }
        return triggeringEventClassNames;
    }

    public boolean isTheSame(ViewObject other) {
        Set<String> copyOfClassnames = new HashSet<>(classNames);
        copyOfClassnames.removeAll(other.classNames);
        if (!copyOfClassnames.isEmpty()) {
            return false;
        }
        copyOfClassnames = new HashSet<>(other.classNames);
        copyOfClassnames.removeAll(classNames);
        if (!copyOfClassnames.isEmpty()) {
            return false;
        }

        Set<String> copyOfViewField = new HashSet<>(valueFields);
        copyOfViewField.removeAll(other.valueFields);
        if (!copyOfViewField.isEmpty()) {
            return false;
        }
        copyOfViewField = new HashSet<>(other.valueFields);
        copyOfViewField.removeAll(valueFields);
        if (!copyOfViewField.isEmpty()) {
            return false;
        }

        if (!isMapOfArraysTheSame(arrayFields, other.arrayFields)) {
            return false;
        }
        if (!isMapOfViewObjectsTheSame(refFields, other.refFields)) {
            return false;
        }
        if (!isMapOfArraysTheSame(backRefFields, other.backRefFields)) {
            return false;
        }
        if (!isMapOfArraysTheSame(countFields, other.countFields)) {
            return false;
        }
        if (!isMapOfViewObjectsTheSame(latestBackRefFields, other.latestBackRefFields)) {
            return false;
        }
        return true;
    }

    private boolean isMapOfArraysTheSame(Map<String, ViewArray> fields, Map<String, ViewArray> otherFields) {
        Set<String> copyOfViewField = new HashSet<>(fields.keySet());
        copyOfViewField.removeAll(otherFields.keySet());
        if (!copyOfViewField.isEmpty()) {
            return false;
        }
        copyOfViewField = new HashSet<>(otherFields.keySet());
        copyOfViewField.removeAll(fields.keySet());
        if (!copyOfViewField.isEmpty()) {
            return false;
        }
        for (Entry<String, ViewArray> e : fields.entrySet()) {
            if (!e.getValue().isSame(otherFields.get(e.getKey()))) {
                return false;
            }
        }
        return true;
    }

    private boolean isMapOfViewObjectsTheSame(Map<String, ViewObject> fields, Map<String, ViewObject> otherFields) {
        Set<String> copyOfViewField = new HashSet<>(fields.keySet());
        copyOfViewField.removeAll(otherFields.keySet());
        if (!copyOfViewField.isEmpty()) {
            return false;
        }
        copyOfViewField = new HashSet<>(otherFields.keySet());
        copyOfViewField.removeAll(fields.keySet());
        if (!copyOfViewField.isEmpty()) {
            return false;
        }
        for (Entry<String, ViewObject> e : fields.entrySet()) {
            if (!e.getValue().isTheSame(otherFields.get(e.getKey()))) {
                return false;
            }
        }
        return true;
    }

    public void depthFirstTraverse(PathCallback pathCallback) {
        if (!valueFields.isEmpty()) {
            pathCallback.push(classNames, ValueType.value, valueFields.toArray(new String[valueFields.size()]));
            pathCallback.pop();
        }
        if (!arrayFields.isEmpty()) {
            for (Entry<String, ViewArray> arrayField : arrayFields.entrySet()) {
                pathCallback.push(classNames, ValueType.refs, arrayField.getKey());
                arrayField.getValue().depthFirstTravers(pathCallback);
                pathCallback.pop();
            }
        }
        if (!refFields.isEmpty()) {
            for (Entry<String, ViewObject> refField : refFields.entrySet()) {
                pathCallback.push(classNames, ValueType.ref, refField.getKey());
                refField.getValue().depthFirstTraverse(pathCallback);
                pathCallback.pop();
            }
        }
        if (!backRefFields.isEmpty()) {
            for (Entry<String, ViewArray> backRefsField : backRefFields.entrySet()) {
                pathCallback.push(classNames, ValueType.backrefs, backRefsField.getKey());

                //We have no way to get the referring field type from the example json so we use refOrRefs value type
                //ViewObject referringObject = backRefsField.getValue().element;
                //pathCallback.push(referringObject.id.getClassName(), ValueType.refOrRefs, backRefsField.getKey());

                backRefsField.getValue().depthFirstTravers(pathCallback);
                pathCallback.pop();
            }
        }
        if (!countFields.isEmpty()) {
            for (Entry<String, ViewArray> countField : countFields.entrySet()) {
                pathCallback.push(classNames, ValueType.count, countField.getKey());

                //We have no way to get the referring field type from the example json so we use refOrRefs value type
                //ViewObject referringObject = countField.getValue().element;
                //pathCallback.push(referringObject.id.getClassName(), ValueType.refOrRefs, backRefsField.getKey());

                countField.getValue().depthFirstTravers(pathCallback);
                pathCallback.pop();
            }
        }
        if (!latestBackRefFields.isEmpty()) {
            for (Entry<String, ViewObject> lastestBackRefField : latestBackRefFields.entrySet()) {
                pathCallback.push(classNames, ValueType.latest_backref, lastestBackRefField.getKey());
                lastestBackRefField.getValue().depthFirstTraverse(pathCallback);
                pathCallback.pop();
            }
        }
    }

    Map<String, ViewObject> getRefFields() {
        return refFields;
    }

    Map<String, ViewArray> getBackRefFields() {
        return backRefFields;
    }

    @Override
    public String toString() {
        return "ViewObject{" + "classNames=" + classNames
            + ", valueFields=" + valueFields
            + ", arrayFields=" + arrayFields
            + ", refFields=" + refFields
            + ", backRefFields=" + backRefFields
            + ", latestBackRefFields=" + latestBackRefFields + '}';
    }

    @Override
    public int hashCode() {
        int hash = 3;
        hash = 53 * hash + Objects.hashCode(this.classNames);
        hash = 53 * hash + Objects.hashCode(this.valueFields);
        hash = 53 * hash + Objects.hashCode(this.arrayFields);
        hash = 53 * hash + Objects.hashCode(this.refFields);
        hash = 53 * hash + Objects.hashCode(this.backRefFields);
        hash = 53 * hash + Objects.hashCode(this.latestBackRefFields);
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
        final ViewObject other = (ViewObject) obj;
        if (!Objects.equals(this.classNames, other.classNames)) {
            return false;
        }
        if (!Objects.equals(this.valueFields, other.valueFields)) {
            return false;
        }
        if (!Objects.equals(this.arrayFields, other.arrayFields)) {
            return false;
        }
        if (!Objects.equals(this.refFields, other.refFields)) {
            return false;
        }
        if (!Objects.equals(this.backRefFields, other.backRefFields)) {
            return false;
        }
        if (!Objects.equals(this.latestBackRefFields, other.latestBackRefFields)) {
            return false;
        }
        return true;
    }

    public static class ViewArray {

        ViewObject element;

        public void depthFirstTravers(PathCallback pathCallback) {
            element.depthFirstTraverse(pathCallback);
        }

        public boolean isSame(ViewArray other) {
            return element.isTheSame(other.element);
        }

        Collection<? extends String> getTriggeringEventClassNames() {
            return element.getTriggeringEventClassNames();
        }

        @Override
        public String toString() {
            return "ViewArray{" + "element=" + element + '}';
        }

        @Override
        public int hashCode() {
            int hash = 7;
            hash = 29 * hash + Objects.hashCode(this.element);
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
            final ViewArray other = (ViewArray) obj;
            if (!Objects.equals(this.element, other.element)) {
                return false;
            }
            return true;
        }
    }

    public static ViewObjectBuilder builder(ObjectNode exampleViewNode) {
        return new ViewObjectBuilder(exampleViewNode);
    }

    public static class ViewObjectBuilder {

        private final ObjectNode exampleViewNode;

        private ViewObjectBuilder(ObjectNode exampleViewNode) {
            this.exampleViewNode = exampleViewNode;
        }

        public ViewObject build() {

            return processObjectNode(exampleViewNode);
        }

        private ViewObject processObjectNode(ObjectNode objectNode) {

            Set<ObjectId> nodeIds = null;
            Set<String> valueFields = new HashSet<>();
            Map<String, ViewArray> arrayFields = new HashMap<>();
            Map<String, ViewObject> refFields = new HashMap<>();
            Map<String, ViewArray> backRefFields = new HashMap<>();
            Map<String, ViewArray> countFields = new HashMap<>();
            Map<String, ViewObject> latestBackRefFields = new HashMap<>();

            for (Iterator<String> fieldIter = objectNode.fieldNames(); fieldIter.hasNext();) {
                String fieldName = fieldIter.next();
                if (isIgnoredField(fieldName)) {
                    continue;
                }

                JsonNode value = objectNode.get(fieldName);

                if (value == null || value.isNull()) {
                    throw new IllegalArgumentException("All fields present in example view data must have associated values. Field "
                        + fieldName + " does not.");
                }

                if (fieldName.equals(ReservedFields.VIEW_OBJECT_ID)) {
                    String text = value.textValue();
                    if (isValidRefValue(text)) {
                        nodeIds = parseObjectIds(text);
                    } else {
                        throw new IllegalArgumentException("Encountered an instanceId that is not an objectId in string form." + value);
                    }
                } else if (fieldName.startsWith(ReservedFields.ALL_BACK_REF_FIELD_PREFIX) ||
                    fieldName.startsWith(ReservedFields.COUNT_BACK_REF_FIELD_PREFIX)) {
                    if (value.isArray() && value.size() == 1 && value.get(0).isObject()) {
                        ViewArray viewArray = processArrayNode((ArrayNode) value);
                        if (viewArray.element == null) {
                            throw new IllegalArgumentException("Back reference field " + fieldName + " does not contain valid example view data");
                        }
                        if (fieldName.startsWith(ReservedFields.ALL_BACK_REF_FIELD_PREFIX)) {
                            backRefFields.put(removeFieldNameModifier(fieldName), viewArray);
                        } else {
                            countFields.put(removeFieldNameModifier(fieldName), viewArray);
                        }
                    } else {
                        throw new IllegalArgumentException("Back-Reference fields (fields beginning with "
                            + ReservedFields.ALL_BACK_REF_FIELD_PREFIX + " or " + ReservedFields.COUNT_BACK_REF_FIELD_PREFIX
                            + ") must hold"
                            + " values that are one element arrays of json objects. "
                            + fieldName + " does not.");
                    }
                } else if (fieldName.startsWith(ReservedFields.LATEST_BACK_REF_FIELD_PREFIX)) {
                    if (!value.isObject()) {
                        throw new IllegalArgumentException("Latest Back reference field " + fieldName + " does not contain valid example view data");
                    }
                    latestBackRefFields.put(removeFieldNameModifier(fieldName), processObjectNode((ObjectNode) value));
                } else if (value.isArray()) {
                    ViewArray viewArray = processArrayNode((ArrayNode) value);

                    //if there was an object within the array, it is a refs field.
                    //if not it's just an array of literal values and we treat it as
                    //an opaque value field
                    if (viewArray.element != null) {
                        arrayFields.put(fieldName, processArrayNode((ArrayNode) value));
                    } else {
                        valueFields.add(fieldName);
                    }
                } else if (value.isObject()) {
                    refFields.put(fieldName, processObjectNode((ObjectNode) value));
                } else {
                    valueFields.add(fieldName);
                }
            }

            return new ViewObject(nodeIds, valueFields, arrayFields, refFields, backRefFields, countFields, latestBackRefFields);
        }

        private boolean isIgnoredField(String fieldName) {
            return ReservedFields.VIEW_CLASS.equals(fieldName);
        }

        private boolean isValidRefValue(String exampleValue) {
            if (exampleValue == null || "".equals(exampleValue.trim())) {
                return false;
            }

            for (String token : exampleValue.split("\\|")) {
                if (!ObjectId.isStringForm(token)) {
                    return false;
                }
            }

            return true;
        }

        private Set<ObjectId> parseObjectIds(String exampleValue) {
            Set<ObjectId> ids = new HashSet<>();
            for (String token : exampleValue.split("\\|")) {
                ids.add(new ObjectId(token));
            }

            return ids;
        }

        private String removeFieldNameModifier(String fieldname) {
            if (fieldname.startsWith(ReservedFields.ALL_BACK_REF_FIELD_PREFIX)) {
                return fieldname.substring(ReservedFields.ALL_BACK_REF_FIELD_PREFIX.length());
            } else if (fieldname.startsWith(ReservedFields.LATEST_BACK_REF_FIELD_PREFIX)) {
                return fieldname.substring(ReservedFields.LATEST_BACK_REF_FIELD_PREFIX.length());
            } else if (fieldname.startsWith(ReservedFields.COUNT_BACK_REF_FIELD_PREFIX)) {
                return fieldname.substring(ReservedFields.COUNT_BACK_REF_FIELD_PREFIX.length());
            } else {
                return fieldname;
            }
        }

        private ViewArray processArrayNode(ArrayNode arrayNode) {
            ViewArray node = new ViewArray();

            if (arrayNode.size() != 1) {
                throw new IllegalArgumentException("Arrays in example view data need to contain one and only one element");
            }

            JsonNode element = arrayNode.get(0);
            if (element == null || element.isNull()) {
                throw new IllegalArgumentException("Arrays in example view data cannot contain null elements");
            }

            if (element.isArray()) {
                throw new IllegalArgumentException("Arrays in example view data cannot be directly nested within other arrays.");
            } else if (element.isObject()) {
                node.element = processObjectNode((ObjectNode) element);
            }

            return node;
        }
    }
}
