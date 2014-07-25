package com.jivesoftware.os.tasmo.id;

import com.jivesoftware.os.jive.utils.row.column.value.store.marshall.api.TypeMarshaller;

/**
 *
 */
public class SaltingDelegatingMarshaller<M extends TypeMarshaller<T>, T> implements TypeMarshaller<T>  {

    public static final int SALT_SIZE_IN_BYTES = 4;

    private final M delegate;
    private final boolean saltPrefixNotPostfix;

    public SaltingDelegatingMarshaller(M delegate) {
        this(delegate, true);
    }

    public SaltingDelegatingMarshaller(M delegate, boolean saltPrefixNotPostfix) {
        this.delegate = delegate;
        this.saltPrefixNotPostfix = saltPrefixNotPostfix;
    }

    public T fromBytes(byte[] bytes) throws Exception {
        return delegate.fromBytes(unsalt(bytes));
    }

    public byte[] toBytes(T t) throws Exception {
        return salt(delegate.toBytes(t), t);
    }

    public T fromLexBytes(byte[] bytes) throws Exception {
        return delegate.fromLexBytes(unsalt(bytes));
    }

    public byte[] toLexBytes(T t) throws Exception {
        return salt(delegate.toLexBytes(t), t);
    }

    private byte[] salt(byte[] unsaltedBytes, T t) {
        int unsaltedBytesLength = unsaltedBytes.length;
        int saltedBytesLength = unsaltedBytesLength + SALT_SIZE_IN_BYTES;
        byte[] saltedBytes = new byte[saltedBytesLength];

        int hashCode = t.hashCode();
        byte[] salt = new byte[] {
                (byte) (hashCode >>> 24),
                (byte) (hashCode >>> 16),
                (byte) (hashCode >>> 8),
                (byte) hashCode};

        // Copy salt to front or back of saltedBytes depending on saltPrefixNotPostfix
        int saltedBytesPos = saltPrefixNotPostfix ? 0 : saltedBytesLength - SALT_SIZE_IN_BYTES;
        System.arraycopy(salt, 0, saltedBytes, saltedBytesPos, SALT_SIZE_IN_BYTES);

        // Copy unsaltedBytes to back or front of saltedBytes depending on saltPrefixNotPostfix
        saltedBytesPos = saltPrefixNotPostfix ? SALT_SIZE_IN_BYTES : 0;
        System.arraycopy(unsaltedBytes, 0, saltedBytes, saltedBytesPos, unsaltedBytesLength);

        return saltedBytes;
    }

    private byte[] unsalt(byte[] saltedBytes) {
        int saltedBytesLength = saltedBytes.length;
        if (saltedBytesLength < SALT_SIZE_IN_BYTES) {
            throw new IllegalArgumentException("Salted byte array can't be shorter than salt size " + SALT_SIZE_IN_BYTES);
        }

        int unsaltedBytesLength = saltedBytesLength - SALT_SIZE_IN_BYTES;
        byte[] unsaltedBytes = new byte[unsaltedBytesLength];

        // Copy the back or front of saltedBytes to unsaltedBytes depending on saltPrefixNotPostfix
        int saltedBytesPos = saltPrefixNotPostfix ? SALT_SIZE_IN_BYTES : 0;
        System.arraycopy(saltedBytes, saltedBytesPos, unsaltedBytes, 0, unsaltedBytesLength);

        return unsaltedBytes;
    }
}
