package com.intel.producer;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Snapshot;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;


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

  public static void reportStream(
    long totalTime,
    String outputDir,
    String reportName,
    Histogram rpsHistogram,
    Histogram mpsHistogram,
    Counter counter
  ) {
    try {
      File outputFile = new File(outputDir, reportName + ".csv");
      System.out.println("written out metrics to " + outputFile.getCanonicalPath());
      String header = "time,count,min_throughput(records/sec),avg_throughput(records/sec),max_throughput(records/sec)," +
                        "min_throughput(MB/sec),avg_throughput(MB/sec),max_throughput(MB/sec)\n";
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

      Long count = counter.getCount();
      Snapshot rpsSnapshot = rpsHistogram.getSnapshot();
      Snapshot mpsSnapshot = mpsHistogram.getSnapshot();
      outputFileWriter.append(totalTime  + ",")
        .append(count + ",")
        .append(formatLong(rpsSnapshot.getMin()) + ",")
        .append(formatDouble(rpsSnapshot.getMean()) + ",")
        .append(formatLong(rpsSnapshot.getMax()) + ",")
        .append(formatLong(mpsSnapshot.getMin()) + ",")
        .append(formatDouble(mpsSnapshot.getMean()) + ",")
        .append(formatLong(mpsSnapshot.getMax()) + "\n");
      outputFileWriter.close();
    } catch (IOException e) {
      e.printStackTrace();
    }

  }

  public static void report(
    long totalTime,
    long count,
    String outputDir,
    String reportName,
    int numOfThreads,
    Histogram recsPerSecMetrics,
    Histogram mbPerSecMetrics,
    Histogram latencyMetrics
  ) {
    try {
      Random random = new Random(System.currentTimeMillis());
      File outputFile = new File(outputDir, reportName + random.nextInt(1000) + ".csv");
      System.out.println("written out metrics to " + outputFile.getCanonicalPath());
      String header = "time,record count,min_throughput(records/sec),avg_throughput(records/sec),max_throughput(records/sec),"
        + "min_throughput(MB/sec),avg_throughput(MB/sec),max_throughput(MB/sec),min_latency(ms),avg_latency(ms),max_latency(ms)\n";
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

      Snapshot recsPerSecSnapshot = recsPerSecMetrics.getSnapshot();
      Snapshot mbPerSecSnapshot = mbPerSecMetrics.getSnapshot();
      Snapshot latencySnapshot = latencyMetrics.getSnapshot();
      outputFileWriter.append(totalTime  + ",")
        .append(count + ",")
        .append(formatLong(recsPerSecSnapshot.getMin() * numOfThreads) + ",")
        .append(formatDouble(recsPerSecSnapshot.getMean() * numOfThreads) + ",")
        .append(formatLong(recsPerSecSnapshot.getMax() * numOfThreads) + ",")
        .append(formatLong(mbPerSecSnapshot.getMin() * numOfThreads) + ",")
        .append(formatDouble(mbPerSecSnapshot.getMean() * numOfThreads) + ",")
        .append(formatLong(mbPerSecSnapshot.getMax() * numOfThreads) + ",")
        .append(formatLong(latencySnapshot.getMin()) + ",")
        .append(formatDouble(latencySnapshot.getMean()) + ",")
        .append(formatLong(latencySnapshot.getMax()) + "\n");
      outputFileWriter.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public static Histogram getHistogram(String histogramName, MetricRegistry metrics) {
    return metrics.histogram(histogramName);
  }

  public static Counter getCounter(String counterName, MetricRegistry metrics) {
    return metrics.counter(counterName);
  }

  public static String formatDouble(Double d) {
    return String.format("%.3f", d);
  }

  public static String formatLong(Long l) {
    return formatDouble((double)l);
  }
}
