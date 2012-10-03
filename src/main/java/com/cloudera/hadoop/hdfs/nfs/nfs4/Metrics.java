package com.cloudera.hadoop.hdfs.nfs.nfs4;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;

import com.cloudera.hadoop.hdfs.nfs.Bytes;
import com.google.common.collect.Maps;

public class Metrics {
  protected static final Logger LOGGER = Logger.getLogger(Metrics.class);

  private final Map<String, AtomicLong> mMetrics = Maps.newTreeMap();
  private final MetricsPrinter mMetricsPrinter;

  public Metrics() {
    mMetricsPrinter = new MetricsPrinter(mMetrics);
    mMetricsPrinter.setName("MetricsPrinter");
    mMetricsPrinter.setDaemon(true);
    mMetricsPrinter.start();
  }
  /**
   * Create or increment a named counter
   */
  public void incrementMetric(String name, long count) {
    name = name.toUpperCase();
    AtomicLong counter;
    synchronized (mMetrics) {
      counter = mMetrics.get(name);
      if (counter == null) {
        counter = new AtomicLong(0);
        mMetrics.put(name, counter);
      }
    }
    counter.addAndGet(count);
  }
  
  protected static class MetricsPrinter extends Thread {

    Map<String, AtomicLong> mMetrics;

    public MetricsPrinter(Map<String, AtomicLong> metrics) {
      mMetrics = metrics;
    }

    @Override
    public void run() {
      try {
        while (true) {
          Thread.sleep(60000L);
          synchronized (mMetrics) {
            LOGGER.info("Metrics Start");
            for (String key : mMetrics.keySet()) {
              AtomicLong counter = mMetrics.get(key);
              long value = counter.getAndSet(0);
              if (key.contains("_BYTES_")) {
                LOGGER.info("Metric: " + key + " = "
                    + Bytes.toHuman(value / 60L) + "/sec");
              } else {
                LOGGER.info("Metric: " + key + " = " + value);
              }
            }
          }
        }
      } catch (Exception e) {
        LOGGER.error("Metrics Printer Quitting", e);
      }
    }
  }
}
