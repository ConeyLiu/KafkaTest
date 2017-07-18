package com.intel.producer;


import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.MutuallyExclusiveGroup;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.utils.Utils;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static net.sourceforge.argparse4j.impl.Arguments.store;
import static net.sourceforge.argparse4j.impl.Arguments.storeTrue;

public class ProducerPerformance {

  public static void main(String[] args) throws Exception {
    System.out.println("Test started !");
    long start = System.currentTimeMillis();

    ArgumentParser parser = argParser();
    KafkaProducer<byte[], byte[]> producer = null;

    try {
      Namespace res = parser.parseArgs(args);

      /* parse args */
      String topicName = res.getString("topic");
      int numProducers = res.getInt("numProducers") == null ? 1 : res.getInt("numProducers");
      long numRecords = res.getLong("numRecords");
      Integer recordSize = res.getInt("recordSize");
      int throughput = res.getInt("throughput");
      List<String> producerProps = res.getList("producerConfig");
      String producerConfig = res.getString("producerConfigFile");
      String payloadFilePath = res.getString("payloadFile");
      String outputDir = res.getString("outputDir");
      boolean shouldPrintMetrics = res.getBoolean("printMetrics");

      // since default value gets printed with the help text,
      // we are escaping \n there and replacing it with correct value here.
      String payloadDelimiter = res.getString("payloadDelimiter")
                                  .equals("\\n") ? "\n" : res.getString("payloadDelimiter");

      if (producerProps == null && producerConfig == null) {
        throw new ArgumentParserException("Either --producer-props or --producer.config must be specified.", parser);
      }

      List<byte[]> payloadByteList = new ArrayList<>();
      if (payloadFilePath != null) {
        Path path = Paths.get(payloadFilePath);
        System.out.println("Reading payloads from: " + path.toAbsolutePath());
        if (Files.notExists(path) || Files.size(path) == 0)  {
          throw new  IllegalArgumentException("File does not exist or empty file provided.");
        }

        String[] payloadList = new String(Files.readAllBytes(path), "UTF-8").split(payloadDelimiter);

        System.out.println("Number of messages read: " + payloadList.length);

        for (String payload : payloadList) {
          payloadByteList.add(payload.getBytes(StandardCharsets.UTF_8));
        }
      }

      Properties props = new Properties();
      if (producerConfig != null) {
        props.putAll(Utils.loadProps(producerConfig));
      }
      if (producerProps != null)
        for (String prop : producerProps) {
          String[] pieces = prop.split("=");
          if (pieces.length != 2)
            throw new IllegalArgumentException("Invalid property: " + prop);
          props.put(pieces[0], pieces[1]);
        }

      props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
      props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
      //props.put(ProducerConfig.ACKS_CONFIG, "1");
      producer = new KafkaProducer<byte[], byte[]>(props);

      MetricRegistry metrics = MetricsUtil.getMetrics();
      Histogram recordPerSecondMetrics = MetricsUtil.getHistogram("record_per_second", metrics);
      Histogram sizePerSecondMetrics = MetricsUtil.getHistogram("size_per_second", metrics);
      Histogram latencyMetrics = MetricsUtil.getHistogram("latency", metrics);

      // thread pool for producer
      ExecutorService executor = Executors.newFixedThreadPool(numProducers);
      Future<Long>[] futures = new Future[numProducers];

      for (int i = 0; i < numProducers; i ++) {
        Callable<Long> callable = getProducerThread(
          "producer-" + i,
          numRecords,
          recordSize,
          payloadFilePath,
          topicName,
          shouldPrintMetrics,
          throughput,
          payloadByteList,
          producer,
          recordPerSecondMetrics,
          sizePerSecondMetrics,
          latencyMetrics
        );
        futures[i] = executor.submit(callable);
      }

      executor.shutdown();

      long totalRecord = 0L;
      for (Future<Long> f : futures) {
        totalRecord += f.get();
      }

      producer.flush();

      MetricsUtil.report(
        System.currentTimeMillis() - start,
        totalRecord,
        outputDir,
        "producer_performance",
        numProducers,
        recordPerSecondMetrics,
        sizePerSecondMetrics,
        latencyMetrics);

      System.out.println("Test finished !");
    } catch (ArgumentParserException e) {
      if (args.length == 0) {
        parser.printHelp();
        System.exit(0);
      } else {
        parser.handleError(e);
        System.exit(1);
      }
    } finally {
      if (producer != null) {
        producer.close();
      }
    }

  }

  private static Callable<Long> getProducerThread(
    String threadName,
    long numRecords,
    int recordSize,
    String payloadFilePath,
    String topicName,
    Boolean shouldPrintMetrics,
    int throughputLimit,
    List<byte[]> payloadByteList,
    KafkaProducer<byte[], byte[]> producer,
    Histogram recsPerSecMetrics,
    Histogram mbPerSecMetrics,
    Histogram lantencyMetrics
  ) {

    Callable<Long> producerThread = new Callable<Long>() {

      @Override
      public Long call() throws Exception {
        byte[] payload = new byte[recordSize];
        Random random = new Random(0);

        for (int i = 0; i < payload.length; ++i) {
          payload[i] = (byte) (random.nextInt(26) + 65);
        }

        ProducerRecord<byte[], byte[]> record;
        Stats stats = new Stats(
          threadName,
          numRecords,
          5000,
          recsPerSecMetrics,
          mbPerSecMetrics,
          lantencyMetrics);
        long startMs = System.currentTimeMillis();

        ThroughputThrottler throttler = new ThroughputThrottler(throughputLimit, startMs);
        for (int i = 0; i < numRecords; i++) {
          if (payloadFilePath != null) {
            payload = payloadByteList.get(random.nextInt(payloadByteList.size()));
          }
          record = new ProducerRecord<>(topicName, payload);

          long sendStartMs = System.currentTimeMillis();
          Callback cb = stats.nextCompletion(sendStartMs, payload.length, stats);
          producer.send(record, cb);

          if (throttler.shouldThrottle(i, sendStartMs)) {
            throttler.throttle();
          }
        }

        producer.flush();
        return stats.getCount();
      }
    };

    return producerThread;
  }

  /** Get the command-line argument parser. */
  private static ArgumentParser argParser() {
    ArgumentParser parser = ArgumentParsers
                              .newArgumentParser("producer-performance")
                              .defaultHelp(true)
                              .description("This tool is used to verify the producer performance.");

    MutuallyExclusiveGroup payloadOptions = parser
                                              .addMutuallyExclusiveGroup()
                                              .required(true)
                                              .description("either --record-size or --payload-file must be specified but not both.");

    parser.addArgument("--topic")
      .action(store())
      .required(true)
      .type(String.class)
      .metavar("TOPIC")
      .help("produce messages to this topic");

    parser.addArgument("--num-producers")
      .action(store())
      .required(false)
      .type(Integer.class)
      .metavar("NUM-PRODUCERS")
      .dest("numProducers")
      .help("number of producers");

    parser.addArgument("--num-records")
      .action(store())
      .required(true)
      .type(Long.class)
      .metavar("NUM-RECORDS")
      .dest("numRecords")
      .help("number of messages to produce");

    payloadOptions.addArgument("--record-size")
      .action(store())
      .required(false)
      .type(Integer.class)
      .metavar("RECORD-SIZE")
      .dest("recordSize")
      .help("message size in bytes. Note that you must provide exactly one of --record-size or --payload-file.");

    payloadOptions.addArgument("--payload-file")
      .action(store())
      .required(false)
      .type(String.class)
      .metavar("PAYLOAD-FILE")
      .dest("payloadFile")
      .help("file to read the message payloads from. This works only for UTF-8 encoded text files. " +
              "Payloads will be read from this file and a payload will be randomly selected when sending messages. " +
              "Note that you must provide exactly one of --record-size or --payload-file.");

    parser.addArgument("--payload-delimiter")
      .action(store())
      .required(false)
      .type(String.class)
      .metavar("PAYLOAD-DELIMITER")
      .dest("payloadDelimiter")
      .setDefault("\\n")
      .help("provides delimiter to be used when --payload-file is provided. " +
              "Defaults to new line. " +
              "Note that this parameter will be ignored if --payload-file is not provided.");

    parser.addArgument("--throughput")
      .action(store())
      .required(true)
      .type(Integer.class)
      .metavar("THROUGHPUT")
      .help("throttle maximum message throughput to *approximately* THROUGHPUT messages/sec");

    parser.addArgument("--producer-props")
      .nargs("+")
      .required(false)
      .metavar("PROP-NAME=PROP-VALUE")
      .type(String.class)
      .dest("producerConfig")
      .help("kafka producer related configuration properties like bootstrap.servers,client.id etc. " +
              "These configs take precedence over those passed via --producer.config.");

    parser.addArgument("--producer.config")
      .action(store())
      .required(false)
      .type(String.class)
      .metavar("CONFIG-FILE")
      .dest("producerConfigFile")
      .help("producer config properties file.");

    parser.addArgument("--output-dir")
      .action(store())
      .required(true)
      .type(String.class)
      .metavar("OUTPUT-DIR")
      .dest("outputDir")
      .help("the record output dir");

    parser.addArgument("--print-metrics")
      .action(storeTrue())
      .type(Boolean.class)
      .metavar("PRINT-METRICS")
      .dest("printMetrics")
      .help("print out metrics at the end of the test.");

    return parser;
  }

  private static class Stats {
    private String threadName;
    private long start;
    private long windowStart;
    private int iteration;
    private long count;
    private long windowCount;
    private long windowBytes;
    private long reportingInterval;
    private Histogram recsPerSecMetrics;
    private Histogram mbPerSecMetrics;
    private Histogram lantencyMetrics;
    private Boolean firstRecord = true;

    public Stats(String threadName,
                 long numRecords,
                 int reportingInterval,
                 Histogram recsPerSecMetrics,
                 Histogram mbPerSecMetrics,
                 Histogram lantencyMetrics) {
      this.threadName = threadName;
      this.start = System.currentTimeMillis();
      this.windowStart = System.currentTimeMillis();
      this.iteration = 0;
      this.windowCount = 0;
      this.windowBytes = 0;
      this.reportingInterval = reportingInterval;
      this.recsPerSecMetrics = recsPerSecMetrics;
      this.mbPerSecMetrics = mbPerSecMetrics;
      this.lantencyMetrics = lantencyMetrics;
    }

    public void record(int iter, int latency, int bytes, long time) {
      if (firstRecord) {
        firstRecord = false;
        newWindow();
      }

      this.count++;
      this.windowCount++;
      this.windowBytes += bytes;


      if (time - windowStart >= reportingInterval) {
        lantencyMetrics.update(latency);
        recordWindow();
        newWindow();
      }
    }

    public Callback nextCompletion(long start, int bytes, Stats stats) {
      Callback cb = new PerfCallback(this.iteration, start, bytes, stats);
      this.iteration++;
      return cb;
    }

    public void recordWindow() {
      long elapsed = System.currentTimeMillis() - windowStart;
      double recsPerSec = 1000.0 * windowCount / (double) elapsed;
      double mbPerSec = 1000.0 * this.windowBytes / (double) elapsed / (1024.0 * 1024.0);
      recsPerSecMetrics.update((long)recsPerSec);
      mbPerSecMetrics.update((long)mbPerSec);
    }

    public void newWindow() {
      this.windowStart = System.currentTimeMillis();
      this.windowCount = 0;
      this.windowBytes = 0;
    }


    public long getCount() {
      return count;
    }

    private static int[] percentiles(int[] latencies, int count, double... percentiles) {
      int size = Math.min(count, latencies.length);
      Arrays.sort(latencies, 0, size);
      int[] values = new int[percentiles.length];
      for (int i = 0; i < percentiles.length; i++) {
        int index = (int) (percentiles[i] * size);
        values[i] = latencies[index];
      }
      return values;
    }
  }

  private static final class PerfCallback implements Callback {
    private final long start;
    private final int iteration;
    private final int bytes;
    private final Stats stats;

    public PerfCallback(int iter, long start, int bytes, Stats stats) {
      this.start = start;
      this.stats = stats;
      this.iteration = iter;
      this.bytes = bytes;
    }

    public void onCompletion(RecordMetadata metadata, Exception exception) {
      long now = System.currentTimeMillis();
      int latency = (int) (now - start);
      this.stats.record(iteration, latency, bytes, now);
      if (exception != null)
        exception.printStackTrace();
    }
  }

}
