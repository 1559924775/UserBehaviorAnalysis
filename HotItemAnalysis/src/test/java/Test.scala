import java.sql.Timestamp

object Test {
  def main(args:Array[String]):Unit={
    //1585790448482，所以系统时间默认到毫秒
    var timeLong=System.currentTimeMillis()
    println(timeLong)
    var timeStamp=new Timestamp(timeLong)
    println(timeStamp)
  }
}
