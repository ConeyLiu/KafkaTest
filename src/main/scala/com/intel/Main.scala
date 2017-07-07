package com.intel

import java.io.IOException
import java.util.{Properties, Random}
import java.lang.{Boolean => jBoolean}

import com.codahale.metrics.{Counter, Histogram}
import joptsimple.OptionParser
import org.apache.kafka.clients.consumer.ConsumerConfig

import scala.collection.JavaConverters._
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable


object Main {
	def main(args: Array[String]): Unit = {
    println("Test started !")

    var config: StreamingConfig = null
    var rpsHistogram: Histogram = null
    var mpsHistogram: Histogram = null
    var count: Counter = null
    var occurError = true
    val start = System.currentTimeMillis()
    try {

      config = new StreamingConfig(args)
      val batchInterval = config.batchInterval.toInt
      val conf = new SparkConf().setAppName("Kafka Test " + System.currentTimeMillis())
      val ssc = new StreamingContext(conf, Seconds(batchInterval))

      val topic = config.topic

      // histogram metrics recorder
      val metrics = MetricsUtil.getMetrics
      rpsHistogram = MetricsUtil.getHistogram("streaming_record_per_second_" + topic, metrics)
      mpsHistogram = MetricsUtil.getHistogram("streaming_mb_per_second_" + topic, metrics)
      count = MetricsUtil.getCounter("streaming_count_" + topic, metrics)

      val kafkaData = KafkaUtils.createDirectStream[Array[Byte], Array[Byte]](
        ssc,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[Array[Byte], Array[Byte]](Array(topic), config.props)
      )

      if (!config.randomRead) {
        kafkaData.foreachRDD { rdd =>
          val first = rdd.first()
          val sizePerRecord = {
            var count = 0
            if (first.key() != null) {
              count += first.key().length
            }
            if (first.value() != null) {
              count += first.value().length
            }
            count
          }

          val count = rdd.count()
          val mbRead = (count * sizePerRecord) / (1024 * 1024)
          rpsHistogram.update(count / config.batchInterval.toInt)
          mpsHistogram.update(mbRead / config.batchInterval.toInt)
        }
      } else {
        // map: partitionID -> maxEndOffSet
        val offsetMap = new mutable.HashMap[Int, Long]()
        kafkaData.foreachRDD { rdd =>
          val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
          if (offsetMap.nonEmpty) {
            val random = new Random()
            val randomOffset = rdd.partitions.map { p =>
              // update the offset
              val offsetRange = offsetRanges(p.index)
              if (offsetRange.untilOffset > offsetMap.getOrElse(p.index, 0L)) {
                offsetMap.put(p.index, offsetRange.untilOffset)
              }

              val endOffset = random.nextInt(offsetMap.getOrElse(p.index, 0L).toInt)
              val startOffset = if ((endOffset - 2000) >= 0) {
                endOffset - 2000
              } else {
                0
              }
              OffsetRange(topic, p.index, startOffset, endOffset)
            }

            val randomRdd = KafkaUtils.createRDD[Array[Byte], Array[Byte]](
              ssc.sparkContext,
              config.props.asJava,
              randomOffset,
              LocationStrategies.PreferConsistent
            ).map(record => (record.key(), record.value()))

            val kvRdd = rdd.map(record => (record.key(), record.value()))
            val unionRDD = randomRdd.union(kvRdd)
            val count = unionRDD.count()

            val first = unionRDD.first()
            val sizePerRecord = {
              var count = 0
              if (first._1 != null) {
                count += first._1.length
              }
              if (first._2 != null) {
                count += first._2.length
              }
              count
            }

            val mbRead = (count * sizePerRecord) / (1024 * 1024)
            rpsHistogram.update(count / config.batchInterval.toInt)
            mpsHistogram.update(mbRead / config.batchInterval.toInt)
          }
        }
      }


      ssc.start()
      occurError = false
      ssc.awaitTermination()

      println("Test finished!")
    } catch {
      case ie: IOException => {
        ie.printStackTrace()
        System.exit(-1)
      }

      case e: Throwable => {
        e.printStackTrace()
        System.exit(-2)
      }
    } finally {
      if (config != null && rpsHistogram != null && mpsHistogram != null && count != null) {
        MetricsUtil.reportStream(
          System.currentTimeMillis() - start,
          config.outputDir,
          "streaming" + config.whetherRandom,
          rpsHistogram,
          mpsHistogram,
          count)
      }
    }
	}

  class StreamingConfig(args: Array[String]) extends OptionParser {
    val parser = new OptionParser(false)
    val batchIntervalOpt = parser.accepts("batchInterval", "REQUIRED: The batch interval used for streaming")
      .withRequiredArg()
      .describedAs("batchInterval")
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
      .defaultsTo("false")
      .ofType(classOf[Boolean])
    val outputDirOpt = parser.accepts("outputDir", "REQUIRED: The report output dir.")
      .withRequiredArg()
      .describedAs("outputDir")
      .ofType(classOf[String])
    val fetchSizeOpt = parser.accepts("fetch-size", "The amount of data to fetch in a single request.")
      .withRequiredArg
      .describedAs("size")
      .ofType(classOf[java.lang.Integer])
      .defaultsTo(1024 * 1024)
    val socketBufferSizeOpt = parser.accepts("socket-buffer-size", "The size of the tcp RECV size.")
      .withRequiredArg
      .describedAs("size")
      .ofType(classOf[java.lang.Integer])
      .defaultsTo(2 * 1024 * 1024)

    val options = parser.parse(args: _*)



    val randomRead = options.valueOf(randomReadOpt).booleanValue()
    val batchInterval = options.valueOf(batchIntervalOpt)
    val topic = options.valueOf(topicOpt)
    val outputDir = options.valueOf(outputDirOpt).toString

    val whetherRandom = if (randomRead) {
      "_random"
    } else {
      ""
    }

    val props = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> options.valueOf(bootstrapServersOpt).toString,
      ConsumerConfig.GROUP_ID_CONFIG -> (options.valueOf(groupIdOpt) + whetherRandom),
      ConsumerConfig.RECEIVE_BUFFER_CONFIG -> options.valueOf(socketBufferSizeOpt).toString,
      ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG -> options.valueOf(fetchSizeOpt).toString,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[ByteArrayDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[ByteArrayDeserializer],
      ConsumerConfig.CHECK_CRCS_CONFIG -> (false: jBoolean),
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> (false: jBoolean)
    )
  }
}
