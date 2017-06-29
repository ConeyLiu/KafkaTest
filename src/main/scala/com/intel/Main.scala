package com.intel

import java.io.{File, FileWriter, IOException}
import java.util.{Date, Properties}

import com.codahale.metrics.{Histogram, MetricRegistry, UniformReservoir}

import com.intel.MetricsUtil.{formatDouble, formatLong}

import scala.collection.JavaConverters._
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable
import scala.util.Random


object Main {
	def main(args: Array[String]): Unit = {
    var topic: String = null
    var outputDir: String = null
    var histogram: Histogram = null
    var occurError = true
    try {
      // load properties
      val properties = new Properties()
      properties.load(getClass.getResourceAsStream("conf.properties"))

      val batchInterval = properties.getProperty("batchInterval").toInt
      val conf = new SparkConf().setAppName("Kafka Test")
      val ssc = new StreamingContext(conf, Seconds(batchInterval))

      val kafkaParams = Map[String, Object](
        "bootstrap.servers" -> properties.getProperty("bootstrap.servers"),
        "key.deserializer" -> classOf[StringDeserializer],
        "value.deserializer" -> classOf[StringDeserializer],
        "group.id" -> properties.getProperty("group.id"),
        "enable.auto.commit" -> (false: java.lang.Boolean)
      )

      // only one topic
      topic = properties.getProperty("topic")
      assert(topic != null)

      outputDir = properties.getProperty("outputDir")
      assert(outputDir != null)

      // histogram metrics recorder
      val metrics = MetricsUtil.getMetrics
      histogram = MetricsUtil.getHistogram("streaming-" + topic, metrics)
      assert(histogram != null)

      val kafkaData = KafkaUtils.createDirectStream[String, String](
        ssc,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String, String](Array(topic), kafkaParams)
      )

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
            val startOffset = if ((endOffset - 100) > 0) {
              endOffset - 100
            } else {
              0
            }
            OffsetRange(topic, p.index, startOffset, endOffset)
          }

          val randomRdd = KafkaUtils.createRDD[String, String](
            ssc.sparkContext,
            kafkaParams.asJava,
            randomOffset,
            LocationStrategies.PreferConsistent
          ).map(record => (record.key(), record.value()))

          val kvRdd = rdd.map(record => (record.key(), record.value()))
          val unionRDD = randomRdd.union(kvRdd)
          val count = unionRDD.count()
          val throughput = count / batchInterval
          histogram.update(throughput)
        }
      }

      ssc.start()
      occurError = false
      ssc.awaitTermination()
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
      if (!occurError) {
        MetricsUtil.reportHistogram(outputDir, topic, histogram)
      }
    }
	}
}
