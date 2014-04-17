package com.jivesoftware.os.tasmo.id;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.base.Preconditions;

/**
 * Typed wrapper around an objectId.
 *
 * @param <T>
 */
public class Ref<T> {

    private final ObjectId objectId;

    @JsonCreator
    public Ref(String stringForm) {
        this(new ObjectId(stringForm));
    }

    private Ref(ObjectId objectId) {
        this.objectId = objectId;
    }

    public Ref(Id id, Class<? super T> cl) {
        this(new ObjectId(cl.getSimpleName(), id), cl);
    }

    public Ref(ObjectId objectId, Class<? super T> cl) {
        Preconditions.checkArgument(objectId.getClassName().equals(cl.getSimpleName()),
            "ObjectId " + objectId + " is not of expected class " + cl.getSimpleName());
        this.objectId = objectId;
    }

    public ObjectId getObjectId() {
        return objectId;
    }

    public Id getId() {
        return objectId.getId();
    }

    @Override
    public String toString() {
        return objectId.toString();
    }

    @JsonValue
    public String toStringForm() {
        return objectId.toStringForm();
    }

    public static <T> Ref<T> fromId(Id id, Class<? super T> cl) {
        return new Ref<>(new ObjectId(cl.getSimpleName(), id), cl);
    }

    public static <T> Ref<T> fromId(String id, Class<? super T> cl) {
        return new Ref<>(new ObjectId(cl.getSimpleName(), new Id(id)), cl);
    }

    public static <T> Ref<T> fromId(long id, Class<? super T> cl) {
        return new Ref<>(new ObjectId(cl.getSimpleName(), new Id(id)), cl);
    }

    public static <T> Ref<T> fromObjectId(ObjectId objectId, Class<? super T> cl) {
        return new Ref<>(objectId, cl);
    }

    public static <T> Ref<T> fromObjectId(String objectId, Class<? super T> cl) {
        return new Ref<>(new ObjectId(objectId), cl);
    }

    public static <T> Ref<T> castObjectId(ObjectId unchecked) {
        return new Ref<>(unchecked.toStringForm());
    }

    public static <T> Ref<T> convertToType(ObjectId objectId, Class<? super T> cl) {
        return new Ref<T>(objectId.getId(), cl);
    }

    public static <T> Ref<T> convertToType(Ref<?> objectId, Class<? super T> cl) {
        return new Ref<T>(objectId.getId(), cl);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Ref ref = (Ref) o;

        if (objectId != null ? !objectId.equals(ref.objectId) : ref.objectId != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        return objectId != null ? objectId.hashCode() : 0;
    }
}
