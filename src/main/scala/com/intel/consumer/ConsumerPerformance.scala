package com.intel.consumer

import java.lang.{Long => jLang}
import java.text.SimpleDateFormat
import java.util
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}
import java.util.{Collections, Properties, Random}

import com.codahale.metrics.Histogram
import com.intel.producer.MetricsUtil
import kafka.tools.PerfConfig
import kafka.utils.CommandLineUtils
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRebalanceListener, KafkaConsumer}
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.common.{Metric, MetricName, TopicPartition}
import org.apache.log4j.Logger

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
  *Performance test for the full zookeeper consumer
  */
object ConsumerPerformance {
	private val logger = Logger.getLogger(getClass())

	def main(args: Array[String]): Unit = {
		println("Test started!")

		val start = System.currentTimeMillis()

		val config = new ConsumerPerfConfig(args)
		logger.info("Starting consumer...")
		val totalMessagesRead = new AtomicLong(0)
		val totalBytesRead = new AtomicLong(0)
		//val consumerTimeout = new AtomicBoolean(false)
		var metrics: mutable.Map[MetricName, _ <: Metric] = null

		val metricRegistry = MetricsUtil.getMetrics
                val rpsHistogram = MetricsUtil.getHistogram("random_record_per_second", metricRegistry)
                val mpsHistogram = MetricsUtil.getHistogram("random_mb_per_second", metricRegistry)
                val latencyHistogram = MetricsUtil.getHistogram("random_latency", metricRegistry)

//		if (!config.hideHeader) {
//			if (!config.showDetailedStats)
//				println("start.time, end.time, data.consumed.in.MB, MB.sec, data.consumed.in.nMsg, nMsg.sec")
//			else
//				println("time, threadID, data.consumed.in.MB, MB.sec, data.consumed.in.nMsg, nMsg.sec")
//		}

		var startMs, endMs = 0L
		val consumer = new KafkaConsumer[Array[Byte], Array[Byte]](config.props)
		consumer.subscribe(Collections.singletonList(config.topic))
		startMs = System.currentTimeMillis
		consume(consumer,
                        List(config.topic),
                        config.numMessages,
                        10000,
                        config,
                        totalMessagesRead,
                        totalBytesRead,
                        rpsHistogram,
                        mpsHistogram,
                        latencyHistogram,
			config.randomRead)
		endMs = System.currentTimeMillis

		if (config.printMetrics) {
			metrics = consumer.metrics().asScala
		}
		consumer.close()

		if (config.showDetailedStats) {
			MetricsUtil.report(
				System.currentTimeMillis() - start,
				totalMessagesRead.get(),
				config.outputDir,
				"consumer_report" + config.whetherRandom,
				1,
				rpsHistogram,
				mpsHistogram,
				latencyHistogram
			)
		}

		println("test finished!")

	}

	def consume(consumer: KafkaConsumer[Array[Byte], Array[Byte]],
              topics: List[String],
              count: Long,
              timeout: Long,
              config: ConsumerPerfConfig,
              totalMessagesRead: AtomicLong,
              totalBytesRead: AtomicLong,
              rpsHistogram: Histogram,
              mpsHistogram: Histogram,
              latencyHistogram: Histogram,
              randomReading: Boolean) {
              // Wait for group join, metadata fetch, etc
              val joinTimeout = 10000
              val isAssigned = new AtomicBoolean(false)
              consumer.subscribe(topics.asJava, new ConsumerRebalanceListener {
			def onPartitionsAssigned(partitions: util.Collection[TopicPartition]) {
				isAssigned.set(true)
			}
			def onPartitionsRevoked(partitions: util.Collection[TopicPartition]) {
				isAssigned.set(false)
			}})
		val joinStart = System.currentTimeMillis()
		while (!isAssigned.get()) {
			if (System.currentTimeMillis() - joinStart >= joinTimeout) {
				throw new Exception("Timed out waiting for initial group join.")
			}
			consumer.poll(100)
		}
		consumer.seekToBeginning(Collections.emptyList())


		val oneTopic = topics(0)
		val partitionInfos = consumer.partitionsFor(oneTopic).asScala
		val topicPartitions = partitionInfos.map(pi => new TopicPartition(oneTopic, pi.partition()))
		// because this test just for read data (already store in Kafka), so the beginning offset and end offset
		// are fixed.
		val beginningOffsets = consumer.beginningOffsets(topicPartitions.asJava)
		val endOffsets = consumer.endOffsets(topicPartitions.asJava)

                // Now start the benchmark
                var bytesRead = 0L
                var messagesRead = 0L
                var messagesReadInBatch = 0L
                var lastBytesRead = 0L
                var lastMessagesRead = 0L
                val startMs = System.currentTimeMillis
                var lastReportTime: Long = startMs
                var lastConsumedTime = System.currentTimeMillis
                var currentTimeMillis = lastConsumedTime

		while (messagesRead < count && currentTimeMillis - lastConsumedTime <= timeout) {

			if (randomReading && messagesReadInBatch >= 2000) {
                                messagesReadInBatch = 0L
				seekToRandomPosition(consumer, topicPartitions, beginningOffsets, endOffsets)
			}

                        val start = System.currentTimeMillis()
                        // using 1000 instead of 100, because of the random read need more time
                        val records = consumer.poll(1000).asScala
			// update poll record latency
                        latencyHistogram.update(System.currentTimeMillis() - start)

			currentTimeMillis = System.currentTimeMillis
			if (records.nonEmpty)
				lastConsumedTime = currentTimeMillis
			for (record <- records) {
                        // this is used for random read, but don't have influence on sequence read,
                        // so the count should be equal to `messagesRead` when sequence read.
                        messagesReadInBatch += 1
				messagesRead += 1
				if (record.key != null)
					bytesRead += record.key.size
				if (record.value != null)
					bytesRead += record.value.size
        
				if (currentTimeMillis - lastReportTime >= config.reportingInterval) {
					if (config.showDetailedStats) {
                                                // update metrics
                                                val elapsedMs: Double = currentTimeMillis - lastReportTime
                                                val recordsRead = messagesRead - lastMessagesRead
                                                val mbRead = ((bytesRead - lastBytesRead) * 1.0) /(1024 * 1024)
                                                rpsHistogram.update((1000.0 * recordsRead / elapsedMs).toLong)
                                                mpsHistogram.update((1000.0 * mbRead / elapsedMs).toLong)
                                        }

					lastReportTime = currentTimeMillis
					lastMessagesRead = messagesRead
					lastBytesRead = bytesRead
				}
			}
		}

                // the last batch or the first but ran time less than reportInterval.
                if (currentTimeMillis - lastReportTime > 0) {
                       if (config.showDetailedStats) {
                               // update metrics
                               val elapsedMs: Double = currentTimeMillis - lastReportTime
                               val recordsRead = messagesRead - lastMessagesRead
                               val mbRead = ((bytesRead - lastBytesRead) * 1.0) / (1024 * 1024)
                               rpsHistogram.update((1000.0 * recordsRead / elapsedMs).toLong)
                               mpsHistogram.update((1000.0 * mbRead / elapsedMs).toLong)
                       }

                       lastReportTime = currentTimeMillis
                       lastMessagesRead = messagesRead
                       lastBytesRead = bytesRead
                }

		totalMessagesRead.set(messagesRead)
		totalBytesRead.set(bytesRead)
	}

	def seekToRandomPosition(consumer: KafkaConsumer[Array[Byte], Array[Byte]],
		topicPartitions: mutable.Iterable[TopicPartition],
		beginningOffsets: util.Map[TopicPartition, jLang],
		endOffsets: util.Map[TopicPartition, jLang]): Unit = {
		val random = new Random(System.currentTimeMillis())
		// seek each partition to random position
		topicPartitions.foreach { tp =>
		val beginningOffset = beginningOffsets.get(tp)
		val endOffset = endOffsets.get(tp)
		val seekPosition = beginningOffset + random.nextInt((endOffset - beginningOffset).toInt)
		// seek to random position
			consumer.seek(tp, seekPosition)
		}
	}

	class ConsumerPerfConfig(args: Array[String]) extends PerfConfig(args) {
		val zkConnectOpt = parser.accepts("zookeeper", "REQUIRED (only when using old consumer): The connection string for the zookeeper connection in the form host:port. " +
			"Multiple URLS can be given to allow fail-over. This option is only used with the old consumer.")
			.withRequiredArg
			.describedAs("urls")
			.ofType(classOf[String])
		val bootstrapServersOpt = parser.accepts("broker-list", "REQUIRED (unless old consumer is used): A broker list to use for connecting if using the new consumer.")
			.withRequiredArg()
			.describedAs("host")
			.ofType(classOf[String])
		val topicOpt = parser.accepts("topic", "REQUIRED: The topic to consume from.")
			.withRequiredArg
			.describedAs("topic")
			.ofType(classOf[String])
		val groupIdOpt = parser.accepts("group", "The group id to consume on.")
			.withRequiredArg
			.describedAs("gid")
			.defaultsTo("perf-consumer-" + new Random().nextInt(100000))
			.ofType(classOf[String])
		val randomReadOpt = parser.accepts("randomRead", "Whether random read the data.")
			.withRequiredArg()
			.describedAs("randomRead")
			.ofType(classOf[Boolean])
			.defaultsTo(false)
		val fetchSizeOpt = parser.accepts("fetch-size", "The amount of data to fetch in a single request.")
			.withRequiredArg
			.describedAs("size")
			.ofType(classOf[java.lang.Integer])
			.defaultsTo(1024 * 1024)
		val resetBeginningOffsetOpt = parser.accepts("from-latest", "If the consumer does not already have an established " +
			"offset to consume from, start with the latest message present in the log rather than the earliest message.")
                val outputDirOpt = parser.accepts("outputDir", "REQUIRED: The report output dir.")
			.withRequiredArg()
			.describedAs("outputDir")
			.ofType(classOf[String])
		val socketBufferSizeOpt = parser.accepts("socket-buffer-size", "The size of the tcp RECV size.")
			.withRequiredArg
			.describedAs("size")
			.ofType(classOf[java.lang.Integer])
			.defaultsTo(2 * 1024 * 1024)
		val numThreadsOpt = parser.accepts("threads", "Number of processing threads.")
			.withRequiredArg
			.describedAs("count")
			.ofType(classOf[java.lang.Integer])
			.defaultsTo(10)
		val numFetchersOpt = parser.accepts("num-fetch-threads", "Number of fetcher threads.")
			.withRequiredArg
			.describedAs("count")
			.ofType(classOf[java.lang.Integer])
			.defaultsTo(1)
		val newConsumerOpt = parser.accepts("new-consumer", "Use the new consumer implementation. This is the default.")
		val consumerConfigOpt = parser.accepts("consumer.config", "Consumer config properties file.")
			.withRequiredArg
			.describedAs("config file")
			.ofType(classOf[String])
		val printMetricsOpt = parser.accepts("print-metrics", "Print out the metrics. This only applies to new consumer.")

		val options = parser.parse(args: _*)

		CommandLineUtils.checkRequiredArgs(parser, options, topicOpt, numMessagesOpt)

		val useOldConsumer = options.has(zkConnectOpt)
		val printMetrics = options.has(printMetricsOpt)

		val props = if (options.has(consumerConfigOpt)) {
			Utils.loadProps(options.valueOf(consumerConfigOpt))
		} else {
			new Properties
		}

		val numThreads = options.valueOf(numThreadsOpt).intValue
		val topic = options.valueOf(topicOpt)
		val numMessages = options.valueOf(numMessagesOpt).longValue
		val reportingInterval = options.valueOf(reportingIntervalOpt).intValue
		if (reportingInterval <= 0)
			throw new IllegalArgumentException("Reporting interval must be greater than 0.")
		val showDetailedStats = options.has(showDetailedStatsOpt)
		val dateFormat = new SimpleDateFormat(options.valueOf(dateFormatOpt))
		val hideHeader = options.has(hideHeaderOpt)
		val outputDir = options.valueOf(outputDirOpt)
		val randomRead = options.valueOf(randomReadOpt).booleanValue()

		val whetherRandom = if (randomRead) {
			"_random"
		} else {
			""
		}

		CommandLineUtils.checkRequiredArgs(parser, options, bootstrapServersOpt)
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, options.valueOf(bootstrapServersOpt))
		props.put(ConsumerConfig.GROUP_ID_CONFIG, options.valueOf(groupIdOpt) + whetherRandom)
		props.put(ConsumerConfig.RECEIVE_BUFFER_CONFIG, options.valueOf(socketBufferSizeOpt).toString)
		props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, options.valueOf(fetchSizeOpt).toString)
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, if (options.has(resetBeginningOffsetOpt)) "latest" else "earliest")
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[ByteArrayDeserializer])
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[ByteArrayDeserializer])
		props.put(ConsumerConfig.CHECK_CRCS_CONFIG, "false")
	}
}
