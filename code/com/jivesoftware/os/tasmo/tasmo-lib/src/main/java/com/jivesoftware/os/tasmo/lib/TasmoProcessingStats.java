package com.jivesoftware.os.tasmo.lib;

import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.math.stat.descriptive.DescriptiveStatistics;

/**
 *
 * @author jonathan
 */
public class TasmoProcessingStats {

    private static final int STATS_WINDOW = 1000;
    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final Map<String, Map<String, DescriptiveStatistics>> latencyCatagories = new ConcurrentHashMap<>();
    private final Map<String, Map<String, AtomicLong>> tallyCatagories = new ConcurrentHashMap<>();

    public void latency(String catagoryName, String name, long sample) {
        Map<String, DescriptiveStatistics> catagory = latencyCatagories.get(catagoryName);
        if (catagory == null) {
            catagory = new ConcurrentHashMap<>();
            latencyCatagories.put(catagoryName, catagory);
        }
        DescriptiveStatistics descriptiveStatistics = catagory.get(name);
        if (descriptiveStatistics == null) {
            descriptiveStatistics = new DescriptiveStatistics(STATS_WINDOW);
            catagory.put(name, descriptiveStatistics);
        }
        descriptiveStatistics.addValue(sample);
    }

    public void tally(String catagoryName, String name, long sample) {
        Map<String, AtomicLong> catagory = tallyCatagories.get(catagoryName);
        if (catagory == null) {
            catagory = new ConcurrentHashMap<>();
            tallyCatagories.put(catagoryName, catagory);
        }
        AtomicLong atomicLong = catagory.get(name);
        if (atomicLong == null) {
            atomicLong = new AtomicLong();
            catagory.put(name, atomicLong);
        }
        atomicLong.addAndGet(sample);
    }

    public void logStats() {
        List<SortableStat> latencyStats = new ArrayList<>();
        for (String catagoryName : latencyCatagories.keySet()) {
            Map<String, DescriptiveStatistics> catagory = latencyCatagories.get(catagoryName);
            for (String name : catagory.keySet()) {
                DescriptiveStatistics descriptiveStatistics = catagory.get(name);
                double sla = 0;
                logStats(sla, "STATS " + catagoryName + " OF " + name, descriptiveStatistics, "millis", latencyStats);
            }
        }

        Collections.sort(latencyStats);
        for (SortableStat stat : latencyStats) {
            LOG.info(stat.value + " " + stat.name);
        }

        List<SortableStat> tallis = new ArrayList<>();
        for (String catagoryName : tallyCatagories.keySet()) {
            Map<String, AtomicLong> catagory = tallyCatagories.get(catagoryName);
            for (String name : catagory.keySet()) {
                AtomicLong atomicLong = catagory.get(name);
                tallis.add(new SortableStat(atomicLong.get(), "TALLY " + catagoryName + " FOR " + name));
            }
        }

        Collections.sort(tallis);
        for (SortableStat stat : tallis) {
            LOG.info(stat.value + " " + stat.name);
        }
    }

    static class SortableStat implements Comparable<SortableStat> {

        private final double value;
        private final String name;

        public SortableStat(double value, String name) {
            this.value = value;
            this.name = name;
        }

        @Override
        public int compareTo(SortableStat o) {
            return Double.compare(value, o.value);
        }
    }

    private void logStats(double sla, String name, DescriptiveStatistics ds, String units, List<SortableStat> stats) {
        //logStatOverSLA(ds.getMin(), sla, units + " min " + name, stats);
        //logStatOverSLA(ds.getMax(), sla, units + " max " + name, stats);
        logStatOverSLA(ds.getMean(), sla, units + " mean " + name, stats);
        //logStatOverSLA(ds.getVariance(), sla,units + " variance " + name, stats);
        logStatOverSLA(ds.getPercentile(50), sla, units + " 50th " + name, stats);
        logStatOverSLA(ds.getPercentile(75), sla, units + " 75th " + name, stats);
        logStatOverSLA(ds.getPercentile(90), sla, units + " 90th " + name, stats);
        //logStatOverSLA(ds.getPercentile(95), sla, units + " 95th " + name, stats);
        logStatOverSLA(ds.getPercentile(99), sla, units + " 99th " + name, stats);
    }

    private void logStatOverSLA(double value, double sla, String name, List<SortableStat> stats) {
        if (value > sla) {
            stats.add(new SortableStat(value, name));
        }
    }
}
