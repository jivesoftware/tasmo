/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.jivesoftware.os.tasmo.model.path;

import com.google.common.collect.Sets;
import java.util.Arrays;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.AssertJUnit.assertNull;

/**
 *
 * @author pete
 */
public class ViewPathDictionaryTest {

    @Test
    public void testDictionaryRoundTrip() {
        ModelPath.Builder builder = ModelPath.builder("Test");
        ModelPathStep root = new ModelPathStep(true, Sets.newHashSet("A", "B"), "firstRef", ModelPathStepType.ref, Sets.newHashSet("C", "D"), null);
        builder.addPathMember(root);

        ModelPathStep middle = new ModelPathStep(false, Sets.newHashSet("C", "D"), "secondRef", ModelPathStepType.refs, Sets.newHashSet("E", "F"), null);
        builder.addPathMember(middle);

        ModelPathStep leaf = new ModelPathStep(false, Sets.newHashSet("E", "F"), null, ModelPathStepType.value, null, Arrays.asList("value1", "value2"));
        builder.addPathMember(leaf);

        ModelPath path = builder.build();

        ViewPathDictionary dictionary = new ViewPathDictionary(path, new StringHashcodeViewPathKeyProvider());

        String[] combo = {"A", "C", "E"};
        long key = dictionary.pathKeyHashcode(combo);
        String[] result = dictionary.lookupModelPathClasses(key);

        assertEquals(result, combo);

        combo = new String[]{"B", "C", "F"};
        key = dictionary.pathKeyHashcode(combo);
        result = dictionary.lookupModelPathClasses(key);

        assertEquals(result, combo);

        combo = new String[]{"B", "D", "E"};
        key = dictionary.pathKeyHashcode(combo);
        result = dictionary.lookupModelPathClasses(key);

        assertEquals(result, combo);

        combo = new String[]{"B", "B", "E"};
        key = dictionary.pathKeyHashcode(combo);
        result = dictionary.lookupModelPathClasses(key);

        assertNull(result);
    }

    @Test
    public void testWithBackRef() {
        ModelPath.Builder builder = ModelPath.builder("Test");
        ModelPathStep root = new ModelPathStep(true,
            Sets.newHashSet("A", "B"), "rootPointsToMiddle", ModelPathStepType.ref, Sets.newHashSet("C", "D"), null);
        builder.addPathMember(root);

        ModelPathStep middle = new ModelPathStep(false, Sets.newHashSet("E", "F"), "tailPointsToMiddle",
            ModelPathStepType.backRefs, Sets.newHashSet("C", "D"), null);
        builder.addPathMember(middle);

        ModelPathStep leaf = new ModelPathStep(false, Sets.newHashSet("E", "F"), null, ModelPathStepType.value, null, Arrays.asList("value1", "value2"));
        builder.addPathMember(leaf);

        ModelPath path = builder.build();

        ViewPathDictionary dictionary = new ViewPathDictionary(path, new StringHashcodeViewPathKeyProvider());

        String[] combo = {"A", "C", "E"};
        long key = dictionary.pathKeyHashcode(combo);
        String[] result = dictionary.lookupModelPathClasses(key);

        assertEquals(result, combo);

    }
}
