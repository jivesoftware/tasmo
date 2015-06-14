package com.jivesoftware.os.tasmo.configuration;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.jivesoftware.os.jive.utils.id.ObjectId;
import com.jivesoftware.os.tasmo.event.api.ReservedFields;

import java.util.*;
import java.util.Map.Entry;

public class ViewObject {

    private final Set<String> classNames;
    private final Set<String> valueFields;
    private final Set<String> centricValueFields;
    private final Map<String, ViewArray> arrayFields;
    private final Map<String, ViewArray> centricArrayFields;
    private final Map<String, ViewObject> refFields;
    private final Map<String, ViewObject> centricRefFields;
    private final Map<String, ViewArray> backRefFields;
    private final Map<String, ViewArray> centricBackRefFields;
    private final Map<String, ViewArray> countFields;
    private final Map<String, ViewArray> centricCountFields;
    private final Map<String, ViewObject> latestBackRefFields;
    private final Map<String, ViewObject> centricLatestBackRefFields;

    public ViewObject(Set<ObjectId> ids,
        Set<String> valueFields,
        Set<String> centricValueFields,
        Map<String, ViewArray> arrayFields,
        Map<String, ViewArray> centricArrayFields,
        Map<String, ViewObject> refFields,
        Map<String, ViewObject> centricRefFields,
        Map<String, ViewArray> backRefFields,
        Map<String, ViewArray> centricBackRefFields,
        Map<String, ViewArray> countFields,
        Map<String, ViewArray> centricCountFields,
        Map<String, ViewObject> latestBackRefFields,
        Map<String, ViewObject> centricLatestBackRefFields) {

        this.classNames = new HashSet<>(ids.size());
        for (ObjectId objectId : ids) {
            classNames.add(objectId.getClassName());
        }

        this.valueFields = valueFields;
        this.centricValueFields = centricValueFields;
        this.arrayFields = arrayFields;
        this.centricArrayFields = centricArrayFields;
        this.refFields = refFields;
        this.centricRefFields = centricRefFields;
        this.backRefFields = backRefFields;
        this.centricBackRefFields = centricBackRefFields;
        this.countFields = countFields;
        this.centricCountFields = centricCountFields;
        this.latestBackRefFields = latestBackRefFields;
        this.centricLatestBackRefFields = centricLatestBackRefFields;
    }

    public Set<String> getTriggeringEventClassNames() {
        Set<String> triggeringEventClassNames = new HashSet<>();
        for (String className : classNames) {
            triggeringEventClassNames.add(className);
        }
        for (ViewArray viewArray : arrayFields.values()) {
            triggeringEventClassNames.addAll(viewArray.getTriggeringEventClassNames());
        }
        for (ViewArray viewArray : centricArrayFields.values()) {
            triggeringEventClassNames.addAll(viewArray.getTriggeringEventClassNames());
        }
        for (ViewObject viewObject : refFields.values()) {
            triggeringEventClassNames.addAll(viewObject.getTriggeringEventClassNames());
        }
        for (ViewObject viewObject : centricRefFields.values()) {
            triggeringEventClassNames.addAll(viewObject.getTriggeringEventClassNames());
        }
        for (ViewArray viewArray : backRefFields.values()) {
            triggeringEventClassNames.addAll(viewArray.getTriggeringEventClassNames());
        }
        for (ViewArray viewArray : centricBackRefFields.values()) {
            triggeringEventClassNames.addAll(viewArray.getTriggeringEventClassNames());
        }
        for (ViewArray viewArray : countFields.values()) {
            triggeringEventClassNames.addAll(viewArray.getTriggeringEventClassNames());
        }
        for (ViewArray viewArray : centricCountFields.values()) {
            triggeringEventClassNames.addAll(viewArray.getTriggeringEventClassNames());
        }
        for (ViewObject viewObject : latestBackRefFields.values()) {
            triggeringEventClassNames.addAll(viewObject.getTriggeringEventClassNames());
        }
        for (ViewObject viewObject : centricLatestBackRefFields.values()) {
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
        if (!isMapOfArraysTheSame(centricArrayFields, other.centricArrayFields)) {
            return false;
        }
        if (!isMapOfViewObjectsTheSame(refFields, other.refFields)) {
            return false;
        }
        if (!isMapOfViewObjectsTheSame(centricRefFields, other.centricRefFields)) {
            return false;
        }
        if (!isMapOfArraysTheSame(backRefFields, other.backRefFields)) {
            return false;
        }
        if (!isMapOfArraysTheSame(centricBackRefFields, other.centricBackRefFields)) {
            return false;
        }
        if (!isMapOfArraysTheSame(countFields, other.countFields)) {
            return false;
        }
        if (!isMapOfArraysTheSame(centricCountFields, other.centricCountFields)) {
            return false;
        }
        if (!isMapOfViewObjectsTheSame(latestBackRefFields, other.latestBackRefFields)) {
            return false;
        }
        if (!isMapOfViewObjectsTheSame(centricLatestBackRefFields, other.centricLatestBackRefFields)) {
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
        if (!centricValueFields.isEmpty()) {
            pathCallback.push(classNames, ValueType.centric_value, centricValueFields.toArray(new String[centricValueFields.size()]));
            pathCallback.pop();
        }
        if (!arrayFields.isEmpty()) {
            for (Entry<String, ViewArray> arrayField : arrayFields.entrySet()) {
                pathCallback.push(classNames, ValueType.refs, arrayField.getKey());
                arrayField.getValue().depthFirstTravers(pathCallback);
                pathCallback.pop();
            }
        }
        if (!centricArrayFields.isEmpty()) {
            for (Entry<String, ViewArray> arrayField : centricArrayFields.entrySet()) {
                pathCallback.push(classNames, ValueType.centric_refs, arrayField.getKey());
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
        if (!centricRefFields.isEmpty()) {
            for (Entry<String, ViewObject> refField : centricRefFields.entrySet()) {
                pathCallback.push(classNames, ValueType.centric_ref, refField.getKey());
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
        if (!centricBackRefFields.isEmpty()) {
            for (Entry<String, ViewArray> backRefsField : centricBackRefFields.entrySet()) {
                pathCallback.push(classNames, ValueType.centric_backrefs, backRefsField.getKey());

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
        if (!centricCountFields.isEmpty()) {
            for (Entry<String, ViewArray> countField : centricCountFields.entrySet()) {
                pathCallback.push(classNames, ValueType.centric_count, countField.getKey());

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
        if (!centricLatestBackRefFields.isEmpty()) {
            for (Entry<String, ViewObject> lastestBackRefField : centricLatestBackRefFields.entrySet()) {
                pathCallback.push(classNames, ValueType.centric_latest_backref, lastestBackRefField.getKey());
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
        return "ViewObject{" +
                "classNames=" + classNames +
                ", valueFields=" + valueFields +
                ", centricValueFields=" + centricValueFields +
                ", arrayFields=" + arrayFields +
                ", centricArrayFields=" + centricArrayFields +
                ", refFields=" + refFields +
                ", centricRefFields=" + centricRefFields +
                ", backRefFields=" + backRefFields +
                ", centricBackRefFields=" + centricBackRefFields +
                ", countFields=" + countFields +
                ", centricCountFields=" + centricCountFields +
                ", latestBackRefFields=" + latestBackRefFields +
                ", centricLatestBackRefFields=" + centricLatestBackRefFields +
                '}';
    }

    @Override
    public int hashCode() {
        int result = classNames != null ? classNames.hashCode() : 0;
        result = 31 * result + (valueFields != null ? valueFields.hashCode() : 0);
        result = 31 * result + (centricValueFields != null ? centricValueFields.hashCode() : 0);
        result = 31 * result + (arrayFields != null ? arrayFields.hashCode() : 0);
        result = 31 * result + (centricArrayFields != null ? centricArrayFields.hashCode() : 0);
        result = 31 * result + (refFields != null ? refFields.hashCode() : 0);
        result = 31 * result + (centricRefFields != null ? centricRefFields.hashCode() : 0);
        result = 31 * result + (backRefFields != null ? backRefFields.hashCode() : 0);
        result = 31 * result + (centricBackRefFields != null ? centricBackRefFields.hashCode() : 0);
        result = 31 * result + (countFields != null ? countFields.hashCode() : 0);
        result = 31 * result + (centricCountFields != null ? centricCountFields.hashCode() : 0);
        result = 31 * result + (latestBackRefFields != null ? latestBackRefFields.hashCode() : 0);
        result = 31 * result + (centricLatestBackRefFields != null ? centricLatestBackRefFields.hashCode() : 0);
        return result;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ViewObject that = (ViewObject) o;

        if (arrayFields != null ? !arrayFields.equals(that.arrayFields) : that.arrayFields != null) return false;
        if (backRefFields != null ? !backRefFields.equals(that.backRefFields) : that.backRefFields != null)
            return false;
        if (centricArrayFields != null ? !centricArrayFields.equals(that.centricArrayFields) : that.centricArrayFields != null)
            return false;
        if (centricBackRefFields != null ? !centricBackRefFields.equals(that.centricBackRefFields) : that.centricBackRefFields != null)
            return false;
        if (centricCountFields != null ? !centricCountFields.equals(that.centricCountFields) : that.centricCountFields != null)
            return false;
        if (centricLatestBackRefFields != null ? !centricLatestBackRefFields.equals(that.centricLatestBackRefFields) : that.centricLatestBackRefFields != null)
            return false;
        if (centricRefFields != null ? !centricRefFields.equals(that.centricRefFields) : that.centricRefFields != null)
            return false;
        if (centricValueFields != null ? !centricValueFields.equals(that.centricValueFields) : that.centricValueFields != null)
            return false;
        if (classNames != null ? !classNames.equals(that.classNames) : that.classNames != null) return false;
        if (countFields != null ? !countFields.equals(that.countFields) : that.countFields != null) return false;
        if (latestBackRefFields != null ? !latestBackRefFields.equals(that.latestBackRefFields) : that.latestBackRefFields != null)
            return false;
        if (refFields != null ? !refFields.equals(that.refFields) : that.refFields != null) return false;
        if (valueFields != null ? !valueFields.equals(that.valueFields) : that.valueFields != null) return false;

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

        private static final String CENTRIC_PREFIX = "centric_";

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
            Set<String> centricValueFields = new HashSet<>();
            Map<String, ViewArray> arrayFields = new HashMap<>();
            Map<String, ViewArray> centricArrayFields = new HashMap<>();
            Map<String, ViewObject> refFields = new HashMap<>();
            Map<String, ViewObject> centricRefFields = new HashMap<>();
            Map<String, ViewArray> backRefFields = new HashMap<>();
            Map<String, ViewArray> centricBackRefFields = new HashMap<>();
            Map<String, ViewArray> countFields = new HashMap<>();
            Map<String, ViewArray> centricCountFields = new HashMap<>();
            Map<String, ViewObject> latestBackRefFields = new HashMap<>();
            Map<String, ViewObject> centricLatestBackRefFields = new HashMap<>();

            for (Iterator<String> fieldIter = objectNode.fieldNames(); fieldIter.hasNext();) {
                String fieldName = fieldIter.next();
                if (isIgnoredField(fieldName)) {
                    continue;
                }

                JsonNode value = objectNode.get(fieldName);

                boolean centricField = fieldName.startsWith(CENTRIC_PREFIX);
                if (centricField) {
                    fieldName = fieldName.substring(CENTRIC_PREFIX.length());
                }

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
                            if (centricField) {
                                centricBackRefFields.put(removeFieldNameModifier(fieldName), viewArray);
                            } else {
                                backRefFields.put(removeFieldNameModifier(fieldName), viewArray);
                            }
                        } else {
                            if (centricField) {
                                centricCountFields.put(removeFieldNameModifier(fieldName), viewArray);
                            } else {
                                countFields.put(removeFieldNameModifier(fieldName), viewArray);
                            }
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
                    if (centricField) {
                        centricLatestBackRefFields.put(removeFieldNameModifier(fieldName), processObjectNode((ObjectNode) value));
                    } else {
                        latestBackRefFields.put(removeFieldNameModifier(fieldName), processObjectNode((ObjectNode) value));
                    }
                } else if (value.isArray()) {
                    ViewArray viewArray = processArrayNode((ArrayNode) value);

                    //if there was an object within the array, it is a refs field.
                    //if not it's just an array of literal values and we treat it as
                    //an opaque value field
                    if (viewArray.element != null) {
                        if (centricField) {
                            centricArrayFields.put(fieldName, processArrayNode((ArrayNode) value));
                        } else {
                            arrayFields.put(fieldName, processArrayNode((ArrayNode) value));
                        }
                    } else {
                        if (centricField) {
                            centricValueFields.add(fieldName);
                        } else {
                            valueFields.add(fieldName);
                        }
                    }
                } else if (value.isObject()) {
                    if (centricField) {
                        centricRefFields.put(fieldName, processObjectNode((ObjectNode) value));
                    } else {
                        refFields.put(fieldName, processObjectNode((ObjectNode) value));
                    }
                } else {
                    if (centricField) {
                        centricValueFields.add(fieldName);
                    } else {
                        valueFields.add(fieldName);
                    }
                }
            }

            return new ViewObject(nodeIds, valueFields, centricValueFields, arrayFields, centricArrayFields,
                    refFields, centricRefFields, backRefFields, centricBackRefFields, countFields, centricCountFields,
                    latestBackRefFields, centricLatestBackRefFields);
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
