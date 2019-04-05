import kafka.serializer.StringDecoder
import org.apache.spark.streaming._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils
import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, SaveMode}

object ScalaProducerExample extends App {

  val conf = new SparkConf().setMaster("local[2]").setAppName("test")
  val sc = new SparkContext(conf)
  val ssc = new StreamingContext(sc, Seconds(5))
  val sqlContext= new org.apache.spark.sql.SQLContext(sc)
  import sqlContext.implicits._

  val kafkaStreams = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
    ssc,
    Map("metadata.broker.list" -> "127.0.0.1:9092","bootstrap.servers"-> "127.0.0.1:9092"),
    Set("iot")
    )

  kafkaStreams.print()

  // Alert producer
  def alertProducer(device_id: String, parameterName: String, parameter: String, alertMessage: String): Unit = {
    val  props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])


    val producer = new KafkaProducer[String, String](props)
    val key = "akey"
    val value = "Alert! " + parameterName + " has reached " + parameter + ". " + alertMessage
    val TOPIC= 	"alert"


    val record = new ProducerRecord(TOPIC, key, value)
    producer.send(record)
    println("Alert for device n°" + device_id + "! " + parameterName + " has reached " + parameter + ". " + alertMessage)
    producer.close()

  }

  kafkaStreams.foreachRDD { rdd =>
    rdd.toDF.write.format("parquet").mode(SaveMode.Append).save("/home/hwalog/Documents/FunctionalDataProgramming/From_Eric/Last/Project_Func_Prog/Storage")
    rdd.foreach { content =>
      // content._1 is the key
      // content._2 is the value
      val contentList = content._2.split(",")

      val id = contentList(0)
      val battery_level = contentList(1)
      val heart_rate = contentList(2)
      val temperature = contentList(3)
      val lat = contentList(4)
      val long = contentList(5)

      val parquet = sqlContext.read.parquet("tmp/testParquet")
      println("\n\n\n\n\n" + parquet + "\n\n\n\n\n")
      //spark.read.format("parquet").load("/tmp/testParquet").createOrReplaceTempView("table")

      //spark.sql("select count(latitude) from table where latitude>0").collect()
      //spark.sql("select count(latitude) from table where latitude<0").collect()


      // Alerts

      // Person is dead <=> heart rate equals 0
      if(heart_rate.toInt == 0) {
        alertProducer(id, "Heart rate", heart_rate, "The person is now dead.")
      }

      // Battery is low
      if(battery_level.toInt < 25) {
        alertProducer(id, "Battery level", battery_level, "The battery is low.")
      }

      // Battery is dead
      if(battery_level.toInt == 0) {
        alertProducer(id, "Battery level", battery_level, "The battery is dead.")
      }

      // Paris latitude and longitude boundaries
      val lat_boundaries = (48.900335, 48.818749) // [ Uppermost latitude, lowermost latitude ]
      val long_boundaries = (2.262327, 2.413503) // [ Leftmost longitude, rightmost longitude ]

      // The person is out of the zone
      if(lat.toDouble > lat_boundaries._1 || lat.toDouble < lat_boundaries._2
      || long.toDouble < long_boundaries._1 || long.toDouble > long_boundaries._2) {
        alertProducer(id, "Boundary crossed", lat + ", " + long, "The person is out of the zone. EXPLOSION")
      }

    }
  }

  ssc.start()
  ssc.awaitTermination()

}
