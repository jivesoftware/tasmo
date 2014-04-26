package com.jivesoftware.os.tasmo.lib;

import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.math.stat.descriptive.DescriptiveStatistics;

/**
 *
 * @author jonathan
 */
public class TasmoProcessingStats {

    private static final int STATS_WINDOW = 1000;
    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final Map<String, Map<String, DescriptiveStatistics>> catagories = new ConcurrentHashMap<>();

    public void sample(String catagoryName, String name, long sample) {
        Map<String, DescriptiveStatistics> catagory = catagories.get(catagoryName);
        if (catagory == null) {
            catagory = new ConcurrentHashMap();
            catagories.put(catagoryName, catagory);
        }
        DescriptiveStatistics descriptiveStatistics = catagory.get(name);
        if (descriptiveStatistics == null) {
            descriptiveStatistics = new DescriptiveStatistics(STATS_WINDOW);
            catagory.put(name, descriptiveStatistics);
        }
        descriptiveStatistics.addValue(sample);
    }

    public void logStats() {
        List<SortableStat> stats = new ArrayList<>();
        for (String catagoryName : catagories.keySet()) {
            Map<String, DescriptiveStatistics> catagory = catagories.get(catagoryName);
            for (String name : catagory.keySet()) {
                DescriptiveStatistics descriptiveStatistics = catagory.get(name);
                double sla = 100;
                logStats(sla, "STATS " + catagoryName + " OF " + name, descriptiveStatistics, "millis", stats);
            }
        }

        Collections.sort(stats);
        for (SortableStat stat : stats) {
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
        logStatOverSLA(ds.getMin(), sla, units + " min " + name, stats);
        logStatOverSLA(ds.getMax(), sla, units + " max " + name, stats);
        logStatOverSLA(ds.getMean(), sla, units + " mean " + name, stats);
        //logStatOverSLA(ds.getVariance(), sla,units + " variance " + name, stats);
        logStatOverSLA(ds.getPercentile(50), sla, units + " 50th " + name, stats);
        logStatOverSLA(ds.getPercentile(75), sla, units + " 75th " + name, stats);
        logStatOverSLA(ds.getPercentile(90), sla, units + " 90th " + name, stats);
        logStatOverSLA(ds.getPercentile(95), sla, units + " 95th " + name, stats);
        logStatOverSLA(ds.getPercentile(99), sla, units + " 99th " + name, stats);
    }

    private void logStatOverSLA(double value, double sla, String name, List<SortableStat> stats) {
        if (value > sla) {
            stats.add(new SortableStat(value, name));
        }
    }
}
