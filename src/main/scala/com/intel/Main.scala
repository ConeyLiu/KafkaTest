package com.intel

import java.util.Properties

import scala.collection.JavaConverters._
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable
import scala.util.Random


object Main {
	def main(args: Array[String]): Unit = {
		// load properties
		val properties = new Properties()
		try {
			properties.load(getClass.getResourceAsStream("conf.properties"))
		} catch {
			case e: Exception => {
				e.printStackTrace()
				System.exit(-1)
			}
		}

		val batchInterval = properties.getProperty("batchInterval").toInt
		val conf = new SparkConf().setAppName("Kafka Test")
		val ssc = new StreamingContext(conf, Seconds(batchInterval))

		val kafkaParams = Map[String, Object](
			"bootstrap.servers" -> properties.getProperty("bootstrap.servers"),
			"key.deserializer" -> classOf[StringDeserializer],
			"value.deserializer" -> classOf[StringDeserializer],
			"group.id" -> properties.getProperty("group.id"),
			"auto.offset.reset" -> "latest",
			"enable.auto.commit" -> (false: java.lang.Boolean)
		)

		// only one topic
		val topic = properties.getProperty("topic")
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
					val endOffset = random.nextInt(offsetMap.getOrElse(p.index, 0).toInt)
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
				randomRdd.join(kvRdd).count()
			}

			// update the offset
			rdd.partitions.foreach { p =>
				val offsetRange = offsetRanges(p.index)
				if (offsetRange.untilOffset > offsetMap.getOrElse(p.index, 0)) {
					offsetMap.put(p.index, offsetRange.untilOffset)
				}
			}
		}

		ssc.start()
		ssc.awaitTermination()

	}
}
