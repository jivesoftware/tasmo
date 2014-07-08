/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.jivesoftware.os.tasmo.model.path;

import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import java.nio.charset.StandardCharsets;

public class MurmurHashViewPathKeyProvider implements ViewPathKeyProvider {

    @Override
    public long pathKeyHashcode(String[] classes) {
        Hasher hasher = Hashing.murmur3_128(12_345).newHasher();
        for (String clazz : classes) {
            hasher.putBytes(clazz.getBytes(StandardCharsets.UTF_8));
        }
        return hasher.hash().asLong();
    }

    @Override
    public long modelPathHashcode(String modelPathId) {
        Hasher hasher = Hashing.murmur3_128(12_345).newHasher();
        hasher.putString(modelPathId, StandardCharsets.UTF_8);
        return hasher.hash().asLong();
    }
}
