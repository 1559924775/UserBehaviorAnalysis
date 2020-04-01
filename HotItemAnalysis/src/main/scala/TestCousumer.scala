import java.util
import java.util.Properties

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, ConsumerRecords, KafkaConsumer}

object TestCousumer {
  def main(args:Array[String])={
    val properties=new Properties
    properties.put("bootstrap.servers","hadoop2:9092")
    properties.put("group.id", "test")
    properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer")
    val	consumer 	= new KafkaConsumer[String,String](properties)
    consumer.subscribe(util.Arrays.asList("topItems"));

    while (true) {

      var records:ConsumerRecords[String,String] = consumer.poll(100);
    import scala.collection.JavaConversions._
      for (record <- records)
      printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());         }
  }
}
