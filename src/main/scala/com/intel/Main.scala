package com.intel

import java.io.{File, FileWriter, IOException}
import java.util.{Date, Properties}

import com.codahale.metrics.{Histogram, UniformReservoir}

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
      histogram = new Histogram(new UniformReservoir(properties.getProperty("sampleNumber").toInt))
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
            if (offsetRange.untilOffset > offsetMap.getOrElse(p.index, 0)) {
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
        report(outputDir, topic, histogram)
      }
    }


	}

  private def report(
                    outputDir: String,
                    metricsTopic: String,
                    histogram: Histogram
                    ): Unit = {
    val outputFile = new File(outputDir, metricsTopic + ".csv")
    println(s"written out metrics to ${outputFile.getCanonicalPath}")
    val header = "time,count,throughput(r/s),mean_throughput(r/s),min_throughput(r/s)," +
      "stddev_throughput(r/s),p50_throughput(r/s),p75_throughput(r/s),p95_throughput(r/s),p98_throughput(r/s)," +
      "p99_throughput(r/s),p999_throughput(r/s)\n"
    val fileExists = outputFile.exists()
    if (!fileExists) {
      val parent = outputFile.getParentFile
      if (!parent.exists()) {
        parent.mkdirs()
      }
      outputFile.createNewFile()
    }
    val outputFileWriter = new FileWriter(outputFile, true)
    if (!fileExists) {
      outputFileWriter.append(header)
    }
    val time = new Date(System.currentTimeMillis()).toString
    val count = histogram.getCount
    val snapshot = histogram.getSnapshot
    outputFileWriter.append(s"$time,$count," +
      s"${formatDouble(snapshot.getMax)}," +
      s"${formatDouble(snapshot.getMean)}," +
      s"${formatDouble(snapshot.getMin)}," +
      s"${formatDouble(snapshot.getStdDev)}," +
      s"${formatDouble(snapshot.getMedian)}," +
      s"${formatDouble(snapshot.get75thPercentile())}," +
      s"${formatDouble(snapshot.get95thPercentile())}," +
      s"${formatDouble(snapshot.get98thPercentile())}," +
      s"${formatDouble(snapshot.get99thPercentile())}," +
      s"${formatDouble(snapshot.get999thPercentile())}\n")
    outputFileWriter.close()
  }

  private def formatDouble(d: Double): String = {
    "%.3f".format(d)
  }
}
