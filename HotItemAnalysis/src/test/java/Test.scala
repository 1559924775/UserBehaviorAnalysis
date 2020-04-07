import java.sql.Timestamp

import org.apache.flink.streaming.api.windowing.triggers.Trigger

object Test {
  def main(args:Array[String]):Unit={
    //1585790448482，所以系统时间默认到毫秒
    var timeLong=System.currentTimeMillis()
    println(timeLong)
    var timeStamp=new Timestamp(timeLong)
    println(timeStamp)
    var bloom=new Bloom(0)
    val value = bloom.hash("873335",61)

  }
//  def func(num:Long):String={
//    val result=new StringBuilder
//
//  }
}
class Bloom(size :Long) extends Serializable{
  //位图的总大小
  private val cap=if(size>0)size else 1<<27  //默认16M
  def hash(value:String,seed:Int):Long={
    var result=0L
    for(i<- 0 until value.length){
      result=result+seed+value.charAt(i)
    }
    result & (cap-1) //同result % cap
  }
}