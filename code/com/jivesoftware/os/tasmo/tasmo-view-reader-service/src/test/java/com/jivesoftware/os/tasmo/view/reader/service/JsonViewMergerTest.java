package com.jivesoftware.os.tasmo.view.reader.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Arrays;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.Test;

public class JsonViewMergerTest {

    ObjectMapper mapper = new ObjectMapper();

    static public class A {

        public String a = "a";
    }

    static public class B {

        public String b = "b";
    }

    static public class AWithArray {

        public String a = "a";
        public List<A> as = Arrays.asList(new A());
    }

    static public class AB {

        public A A = new A();
        public B B = new B();
    }

    @Test
    public void testMerge() {
        JsonViewMerger instance = new JsonViewMerger(mapper);

        ObjectNode mainNode = mapper.convertValue(new A(), ObjectNode.class);
        ObjectNode updateNode = mapper.convertValue(new A(), ObjectNode.class);
        instance.merge(mainNode, updateNode);
        System.out.println(updateNode);
        Assert.assertEquals("a", mainNode.get("a").asText());

        updateNode = mapper.convertValue(new B(), ObjectNode.class);
        instance.merge(mainNode, updateNode);
        System.out.println(mainNode);
        Assert.assertEquals("a", mainNode.get("a").asText());
        Assert.assertEquals("b", mainNode.get("b").asText());

        updateNode = mapper.convertValue(new AWithArray(), ObjectNode.class);
        instance.merge(mainNode, updateNode);
        System.out.println(mainNode);
        Assert.assertEquals("a", mainNode.get("a").asText());
        Assert.assertEquals("a", mainNode.get("as").get(0).get("a").asText());

        updateNode = mapper.convertValue(new AB(), ObjectNode.class);
        instance.merge(mainNode, updateNode);
        System.out.println(mainNode);
        Assert.assertEquals("a", mainNode.get("A").get("a").asText());

    }
}
