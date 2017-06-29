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
