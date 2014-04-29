package com.jivesoftware.os.tasmo.lib.process.traversal;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 *
 * @author jonathan
 */
public class StepTree {

    Map<StepTraverser, StepTree> map = new ConcurrentHashMap<>();

    public void add(List<StepTraverser> steps) {
        StepTree depth = this;
        for (StepTraverser step : steps) {
            StepTree got = depth.map.get(step);
            if (got == null) {
                got = new StepTree();
                depth.map.put(step, got);
            }
            depth = got;
        }
    }

    @Override
    public String toString() {
        return "StepTree{" + "map=" + map + '}';
    }

    public void print() {
        List<String> prefix = new ArrayList<>();
        print(this, prefix);
    }

    void print(StepTree tree, List<String> prefix) {
        for (StepTraverser step : tree.map.keySet()) {
            prefix.add(step.toString());
            StepTree got = tree.map.get(step);
            if (got == null || got.map.isEmpty()) {
                String path = "";
                for (int i = 0; i < prefix.size(); i++) {
                    String p = prefix.get(i);
                    path += ((i != 0) ? " -- " : "") + p;
                    if (!p.startsWith("-")) {
                        char[] dash = new char[p.length()];
                        Arrays.fill(dash, '\\');
                        prefix.set(i, new String(dash));
                    }
                }
                System.out.println(path);
            } else {
                print(got, prefix);
            }
            prefix.remove(prefix.size() - 1);
        }
    }

}
