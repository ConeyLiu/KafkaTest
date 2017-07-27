package com.intel.consumer

import java.io._
import java.lang.{Long => jLang}
import java.text.SimpleDateFormat
import java.util
import java.util.concurrent.{Callable, Executors}
import java.util.{Collections, Properties, Random}

import kafka.tools.PerfConfig
import kafka.utils.CommandLineUtils
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.common.TopicPartition

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

/**
  *Performance test for the full zookeeper consumer
  */
object ConsumerPerformance {
	def main(args: Array[String]): Unit = {
		println("Test started!")
		val config = new ConsumerPerfConfig(args)
    // used for get the topic partition information
    val consumer = new KafkaConsumer[Array[Byte], Array[Byte]](config.props)
    val topicPartitions = consumer.partitionsFor(config.topic)
    val count = config.numMessages / topicPartitions.size()
    var totalTime = 0L
    var totalReadRecords = 0L
    var totalReadBytes = 0L
    val totalLatency = new ArrayBuffer[Long]()

    val executors = Executors.newFixedThreadPool(topicPartitions.size())

    val futures = topicPartitions.asScala.map{ tp =>
      executors.submit(new Callable[(Array[Long], ArrayBuffer[Long])] {

        override def call(): (Array[Long], ArrayBuffer[Long]) = {
          consume(config, count, new TopicPartition(config.topic, tp.partition()))
        }
      })
    }
    executors.shutdown()

    // wait all thread finished
    futures.map(_.get()).foreach { r =>
      totalTime = Math.max(r._1(0), totalTime)
      totalReadRecords += r._1(1)
      totalReadBytes += r._1(2)
      totalLatency ++= r._2
    }

    val resultPath = config.outputDir + "/" + System.currentTimeMillis() + ".csv"
    val outputStream = new FileOutputStream(resultPath)
    val outputStreamWriter = new OutputStreamWriter(outputStream)

    val sortedLatency = totalLatency.sorted
    val length = sortedLatency.length
    val totalRecordsPerSec = 1000.0 * totalReadRecords / totalTime.toDouble
    val totalMBPerSec = 1000.0 * totalReadBytes / (1024.0 * 1024.0) / totalTime.toDouble
    val totalAvgLatency = sortedLatency.sum / length
    val totalMaxLatency = sortedLatency.max
    val n50 = sortedLatency((length * 0.5).toInt)
    val n95 = sortedLatency((length * 0.95).toInt)
    val n99 = sortedLatency((length * 0.99).toInt)
    val n999 = sortedLatency((length * 0.999).toInt)

    outputStreamWriter.append(("%d records read," +
      " %f records/sec," +
      " %.2f MB/sec," +
      " %d ms avg latency," +
      " %d ms max latency," +
      " %d ms 50th," +
      " %d ms 95th," +
      " %d ms 99th," +
      " %d ms 99.9th.\n"
      ).format(totalReadRecords,
      totalRecordsPerSec,
      totalMBPerSec,
      totalAvgLatency,
      totalMaxLatency,
      n50.toLong,
      n95.toLong,
      n99.toLong,
      n999.toLong
    ))

    outputStreamWriter.flush()
    outputStreamWriter.close()
    outputStream.close()
    consumer.close()
    println("Test finished !")
	}

	def consume(config: ConsumerPerfConfig,
             count: Long,
             topicPartition: TopicPartition): (Array[Long], ArrayBuffer[Long]) = {

    val filePath = config.outputDir + "/" + topicPartition.partition() + ".csv"
    var output: BufferedWriter = null
    try {
      val file = new File(filePath)
      if (file.exists()) {
        throw new Exception(file.getName + " already exists.")
      } else {
        file.createNewFile()
      }
      output = new BufferedWriter(new FileWriter(file))
    } catch {
      case e: Exception => throw new RuntimeException(e.getMessage)
    }


		val consumer = new KafkaConsumer[Array[Byte], Array[Byte]](config.props)
    consumer.assign(List(topicPartition).asJava)
		consumer.seekToBeginning(Collections.emptyList())

		val beginningOffsets = consumer.beginningOffsets(List(topicPartition).asJava)
		var endOffsets = consumer.endOffsets(List(topicPartition).asJava)

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
		val latencies = new ArrayBuffer[Long](count.toInt)
		var windowTotalLatency = 0L
		var windowMaxLatency = 0L

		while (messagesRead < count && currentTimeMillis - lastConsumedTime <= 100000) {

			if (config.randomRead && messagesReadInBatch >= 500) {
        messagesReadInBatch = 0L
				endOffsets = consumer.endOffsets(List(topicPartition).asJava)
				seekToRandomPosition(consumer, List(topicPartition), beginningOffsets, endOffsets)
			}

			val records = consumer.poll(100).asScala

			currentTimeMillis = System.currentTimeMillis()
			if (records.nonEmpty)
				lastConsumedTime = currentTimeMillis
			for (record <- records) {

				if (record.key != null)
					bytesRead += record.key.size
				if (record.value != null)
					bytesRead += record.value.size

				// record latency
				val latency = if ((currentTimeMillis - record.timestamp()) > 0) {
					currentTimeMillis - record.timestamp()
				} else {
					0L
				}

				latencies += latency

				windowTotalLatency += latency
				windowMaxLatency = Math.max(windowMaxLatency, latency)

				// this is used for random read, but don't have influence on sequence read,
				// so the count should be equal to `messagesRead` when sequence read.
				messagesReadInBatch += 1
				messagesRead += 1

				if (currentTimeMillis - lastReportTime >= config.reportingInterval) {
					if (config.showDetailedStats) {
						// update metrics
						val elapsedMs: Double = currentTimeMillis - lastReportTime
						val recordsRead = messagesRead - lastMessagesRead
						val recordsPerSec = 1000.0 * recordsRead / elapsedMs.toDouble
						val mbRead = ((bytesRead - lastBytesRead) * 1.0) /(1024 * 1024)
						val mbPerSec = 1000.0 * mbRead / elapsedMs.toDouble
						val avgLatency = windowTotalLatency.toDouble / recordsRead
						output.append(("%d records read," +
							" %.1f records/sec," +
							" %.2f MB/sec," +
							" %.1f ms avg latency," +
							" %d ms max latency.\n"
							).format(recordsRead, recordsPerSec, mbPerSec, avgLatency, windowMaxLatency))
					}

					windowMaxLatency = 0L
					windowTotalLatency = 0L

					lastReportTime = currentTimeMillis
					lastMessagesRead = messagesRead
					lastBytesRead = bytesRead
				}
			}
		}

		val elapsed = System.currentTimeMillis() - startMs
		val totalRecordsPerSec = 1000.0 * messagesRead / elapsed.toDouble
		val totalMBPerSec = 1000.0 * bytesRead / elapsed.toDouble / (1024.0 * 1024.0)
		val sortedLatencies = latencies.sorted
		val length = sortedLatencies.length
		val totalAvgLatency = sortedLatencies.sum.toDouble / length.toDouble
    output.append("\n")

    val result = Array(elapsed, messagesRead, bytesRead)

		output.append(("%d records read," +
			" %f records/sec," +
			" %.2f MB/sec," +
			" %.2f ms avg latency," +
			" %d ms max latency," +
			" %d ms 50th," +
			" %d ms 95th," +
			" %d ms 99th," +
			" %d ms 99.9th.\n"
			).format(messagesRead,
			totalRecordsPerSec,
			totalMBPerSec,
			totalAvgLatency,
			sortedLatencies.max,
			sortedLatencies((length * 0.5).toInt),
			sortedLatencies((length * 0.95).toInt),
			sortedLatencies((length * 0.99).toInt),
			sortedLatencies((length * 0.999).toInt)))

    consumer.close()
    if (output != null) {
      // ignore exception, just throw it
      output.flush()
      output.close()
    }
    println("Thread-" + topicPartition.partition() + " finished!")
    (result, sortedLatencies)
	}

	def seekToRandomPosition(consumer: KafkaConsumer[Array[Byte], Array[Byte]],
      topicPartitions: List[TopicPartition],
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
			.defaultsTo("perf-consumer-" + new Random().nextInt(Integer.MAX_VALUE))
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

		val options = parser.parse(args: _*)

		CommandLineUtils.checkRequiredArgs(parser, options, topicOpt, numMessagesOpt)

		val props = if (options.has(consumerConfigOpt)) {
			Utils.loadProps(options.valueOf(consumerConfigOpt))
		} else {
			new Properties
		}

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
