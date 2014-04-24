package com.jivesoftware.os.tasmo.lib.report;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.math.stat.descriptive.DescriptiveStatistics;

/**
 *
 * @author jonathan
 */
public class TasmoEdgeReport {

    private final Map<String, Map<TasmoReportEdge, TasmoReportEdgeStats>> graphs = new ConcurrentHashMap<>();

    public void edge(String rootClass, String classNameA, String fieldName, String classNameB, long elapse, int fanOut) {
        Map<TasmoReportEdge, TasmoReportEdgeStats> graph = graphs.get(rootClass);
        if (graph == null) {
            graph = new ConcurrentHashMap<>();
            graphs.put(rootClass, graph);
        }
        TasmoReportEdge edge = new TasmoReportEdge(classNameA, fieldName, classNameB);
        TasmoReportEdgeStats stats = graph.get(edge);
        if (stats == null) {
            stats = new TasmoReportEdgeStats();
            graph.put(edge, stats);
        }
        if (elapse > -1) {
            stats.elapse(elapse);
        }
        if (fanOut > -1) {
            stats.fanOut(fanOut);
        }
    }

    public List<String> getTextReport() {
        ArrayList<String> report = new ArrayList<>();
        ArrayList<String> rootClasses = new ArrayList<>(graphs.keySet());
        Collections.sort(rootClasses);
        for (String rootClass : rootClasses) {
            report.add("Root:" + rootClass);
            Map<TasmoReportEdge, TasmoReportEdgeStats> graph = graphs.get(rootClass);
            ArrayList<TasmoReportEdge> edges = new ArrayList<>(graph.keySet());
            Collections.sort(edges);
            for (TasmoReportEdge edge : edges) {
                report.addAll(edge.toTextReport("    "));
                TasmoReportEdgeStats stats = graph.get(edge);
                report.addAll(stats.toTextReport("        "));
            }
        }
        return report;
    }

    public static class TasmoReportEdge implements Comparable<TasmoReportEdge> {

        private final String classNameA;
        private final String fieldName;
        private final String classNameB;

        public TasmoReportEdge(String classNameA, String fieldName, String classNameB) {
            this.classNameA = classNameA;
            this.fieldName = fieldName;
            this.classNameB = classNameB;
        }

        public Collection<String> toTextReport(String indent) {
            return Arrays.asList(new String[]{
                indent + "Edge: classNameA=" + classNameA + ", fieldName=" + fieldName + ", classNameB=" + classNameB
            });
        }

        @Override
        public String toString() {
            return "TasmoReportEdge{" + "classNameA=" + classNameA + ", fieldName=" + fieldName + ", classNameB=" + classNameB + '}';
        }

        @Override
        public int hashCode() {
            int hash = 7;
            hash = 43 * hash + Objects.hashCode(this.classNameA);
            hash = 43 * hash + Objects.hashCode(this.fieldName);
            hash = 43 * hash + Objects.hashCode(this.classNameB);
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
            final TasmoReportEdge other = (TasmoReportEdge) obj;
            if (!Objects.equals(this.classNameA, other.classNameA)) {
                return false;
            }
            if (!Objects.equals(this.fieldName, other.fieldName)) {
                return false;
            }
            if (!Objects.equals(this.classNameB, other.classNameB)) {
                return false;
            }
            return true;
        }

        @Override
        public int compareTo(TasmoReportEdge o) {
            int i = classNameA.compareTo(o.classNameA);
            if (i != 0) {
                return i;
            }
            i = fieldName.compareTo(o.fieldName);
            if (i != 0) {
                return i;
            }
            return classNameB.compareTo(o.classNameB);
        }

    }

    public static class TasmoReportEdgeStats {

        private final DescriptiveStatistics elapse = new DescriptiveStatistics(1000);
        private final DescriptiveStatistics fanOut = new DescriptiveStatistics(1000);

        public void elapse(long elapse) {
            this.elapse.addValue(elapse);
        }

        public void fanOut(long fanOut) {
            this.fanOut.addValue(fanOut);
        }

        public void reset() {
            this.elapse.clear();
            this.fanOut.clear();
        }

        public Collection<String> toTextReport(String indent) {
            return Arrays.asList(new String[]{
                indent + "Elapse: mean=" + elapse.getMean()
                + ", variance=" + elapse.getVariance()
                + ", 50th=" + elapse.getPercentile(50)
                + ", 75th=" + elapse.getPercentile(75)
                + ", 90th=" + elapse.getPercentile(90)
                + ", 95th=" + elapse.getPercentile(95)
                + ", 99th=" + elapse.getPercentile(99),
                indent + "Elapse: fanOut=" + fanOut.getMean()
                + ", variance=" + fanOut.getVariance()
                + ", 50th=" + fanOut.getPercentile(50)
                + ", 75th=" + fanOut.getPercentile(75)
                + ", 90th=" + fanOut.getPercentile(90)
                + ", 95th=" + fanOut.getPercentile(95)
                + ", 99th=" + fanOut.getPercentile(99)
            });
        }
        //return stats.getPercentile(50);
    }
}
