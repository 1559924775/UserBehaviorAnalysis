import java.util.Properties

import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerConfig, ProducerRecord, RecordMetadata}

object KafkaDataProducer {
  def main(args:Array[String]):Unit={
    txtToKafak("topItems")
  }
  def txtToKafak(topic: String)={
    val properties=new Properties
    properties.put("bootstrap.servers","hadoop2:9092")

    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer")
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](properties)
    val bufferedSource = io.Source.fromFile("D:\\IDEA\\UserBehaviorAnalysis\\HotItemAnalysis\\src\\main\\resources\\UserBehavior.csv")
    for(line <- bufferedSource.getLines()){
      var recode=new ProducerRecord[String,String](topic,line)
      producer.send(recode,new Callback() { //回调函数， 该方法会在 Producer 收到 ack 时调用，为异步调用
        override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
          if (exception == null) println("success->" + metadata.offset)
          else {
            print("==>")
            exception.printStackTrace()
          }
        }
      })
    }
    producer.close()
  }
}
