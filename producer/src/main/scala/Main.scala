import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer._
import org.apache.kafka.common.serialization.StringSerializer
import java.util.Properties
import scala.io.Source

object producer extends App {

	val  props = new Properties()
	
	//method 1
	//props.put("bootstrap.servers", ":9092")
 	//props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
	//props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

 	props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
 	props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
	props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
  	
	val producer = new KafkaProducer[String, String](props)

	val lines = Source.fromFile("/home/hwalog/Documents/FunctionalDataProgramming/From_Eric/Last/Project_Func_Prog/producer/iotdata").getLines.toList.map(x=>{
      		Thread.sleep(5000)
      		val record = new ProducerRecord[String, String]("iot" , x)
      		producer.send(record)
    	}
	)
	producer.close()
}
