package com.jivesoftware.os.tasmo.view.reader.api;

import com.google.common.base.Preconditions;
import com.jivesoftware.os.tasmo.id.BaseView;
import com.jivesoftware.os.tasmo.id.Id;
import com.jivesoftware.os.tasmo.id.ObjectId;
import com.jivesoftware.os.tasmo.id.Ref;

public class ViewId<V extends BaseView<?>> {

    private final Id id;
    private final Class<V> viewClass;

    public ViewId(Id id, Class<V> viewClass) {
        this.id = Preconditions.checkNotNull(id, "id");
        this.viewClass = Preconditions.checkNotNull(viewClass, "viewClass");
    }

    public ObjectId asObjectId() {
        return new ObjectId(viewClass.getSimpleName(), id);
    }

    public Id getId() {
        return id;
    }

    public Class<V> getViewClass() {
        return viewClass;
    }

    public String toStringForm() {
        return asObjectId().toStringForm();
    }

    @Override
    public String toString() {
        return asObjectId().toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ViewId<?> viewId = (ViewId<?>) o;

        if (!id.equals(viewId.id)) {
            return false;
        }
        if (!viewClass.equals(viewId.viewClass)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = id.hashCode();
        result = 31 * result + viewClass.hashCode();
        return result;
    }

    public static <V extends BaseView<?>> ViewId<V> ofObject(Ref<?> object, Class<V> viewClass) {
        return new ViewId<>(object.getId(), viewClass);
    }

    public static <V extends BaseView<?>> ViewId<V> ofObject(ObjectId objectId, Class<V> viewClass) {
        return new ViewId<>(objectId.getId(), viewClass);
    }

    public static <V extends BaseView<?>> ViewId<V> ofId(Id id, Class<V> viewClass) {
        return new ViewId<>(id, viewClass);
    }

}
