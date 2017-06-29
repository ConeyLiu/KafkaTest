package com.intel;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Snapshot;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Date;


public class MetricsUtil {
  private static MetricRegistry metriccs = null;

  public static MetricRegistry getMetrics() {
    if (metriccs == null) {
      synchronized (MetricsUtil.class) {
        if (metriccs == null) {
          metriccs = new MetricRegistry();
        }
      }
    }
    return metriccs;
  }

  public static void reportHistogram(
    String outputDir,
    String metricsName,
    Histogram histogram
  ) {
    try {
      File outputFile = new File(outputDir, metricsName + ".csv");
      System.out.println("written out metrics to " + outputFile.getCanonicalPath());
      String header = "time,count,max_throughput(r/s),mean_throughput(r/s),min_throughput(r/s)," +
                        "stddev_throughput(r/s),p75_throughput(r/s),p999_throughput(r/s)\n";
      boolean fileExists = outputFile.exists();
      if (!fileExists) {
        File parent = outputFile.getParentFile();
        if (!parent.exists()) {
          parent.mkdirs();
        }
        outputFile.createNewFile();
      }
      FileWriter outputFileWriter = new FileWriter(outputFile, true);
      if (!fileExists) {
        outputFileWriter.append(header);
      }
      String time = new Date(System.currentTimeMillis()).toString();
      Long count = histogram.getCount();
      Snapshot snapshot = histogram.getSnapshot();
      outputFileWriter.append(time  + ",")
        .append(count + ",")
        .append(formatLong(snapshot.getMax()) + ",")
        .append(formatDouble(snapshot.getMean()) + ",")
        .append(formatLong(snapshot.getMin()) + ",")
        .append(formatDouble(snapshot.getStdDev()) + ",")
        .append(formatDouble(snapshot.get75thPercentile()) + ",")
        .append(formatDouble(snapshot.get999thPercentile()) + "\n");
      outputFileWriter.close();
    } catch (IOException e) {
      e.printStackTrace();
    }

  }

  public static void reportProducerReport(
    long count,
    String outputDir,
    String reportName,
    Histogram recsPerSecMetrics,
    Histogram mbPerSecMetrics,
    Histogram lantencyMetrics
  ) {
    try {
      File outputFile = new File(outputDir, reportName + ".csv");
      System.out.println("written out metrics to " + outputFile.getCanonicalPath());
      String header = "time,record count,min_ratio(records/sec),avg_ratio(records/sec),max_ratio(records/sec),"
        + "min_ratio(MB/sec),avg_ratio(MB/sec),max_ratio(MB/sec),min_latency(ms),avg_latency(ms),max_latency(ms)\n";
      boolean fileExists = outputFile.exists();
      if (!fileExists) {
        File parent = outputFile.getParentFile();
        if (!parent.exists()) {
          parent.mkdirs();
        }
        outputFile.createNewFile();
      }
      FileWriter outputFileWriter = new FileWriter(outputFile, true);
      if (!fileExists) {
        outputFileWriter.append(header);
      }
      String time = new Date(System.currentTimeMillis()).toString();
      Snapshot recsPerSecSnapshot = recsPerSecMetrics.getSnapshot();
      Snapshot mbPerSecSnapshot = mbPerSecMetrics.getSnapshot();
      Snapshot latencySnapshot = lantencyMetrics.getSnapshot();
      outputFileWriter.append(time  + ",")
        .append(count + ",")
        .append(formatLong(recsPerSecSnapshot.getMin()) + ",")
        .append(formatDouble(recsPerSecSnapshot.getMean()) + ",")
        .append(formatLong(recsPerSecSnapshot.getMax()) + ",")
        .append(formatLong(mbPerSecSnapshot.getMin()) + ",")
        .append(formatDouble(mbPerSecSnapshot.getMean()) + ",")
        .append(formatLong(mbPerSecSnapshot.getMax()) + ",")
        .append(formatLong(latencySnapshot.getMin()) + ",")
        .append(formatDouble(latencySnapshot.getMean()) + ",")
        .append(formatLong(latencySnapshot.getMax()) + "\n");
      outputFileWriter.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public static Histogram getHistogram(String histogramName, MetricRegistry metriccs) {
    return metriccs.histogram(histogramName);
  }

  public static Counter getCounter(String counterName, MetricRegistry metriccs) {
    return metriccs.counter(counterName);
  }

  public static String formatDouble(Double d) {
    return String.format("%.3f", d);
  }

  public static String formatLong(Long l) {
    return String.format("%.3f", l);
  }
}
