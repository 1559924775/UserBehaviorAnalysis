

import java.sql.Timestamp
import java.util.Properties

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, StateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector
import org.apache.kafka.clients.consumer.ConsumerConfig

import scala.collection.mutable.ListBuffer
// flatMap和Map需要引用的隐式转换
import org.apache.flink.api.scala._

/**
 * 输入数据样例类
 */
case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timeStamp: Long)

/**
 * 窗口聚合结果样例类
 */
case class ItemViewCount(ItemId: Long, windowEnd: Long, count: Long)

/**
 * 实时热门商品统计（已浏览量为依据）  每隔5分钟输出最近1小时的
 */
object HotItems {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //设置时间语义，默认是ProcessingTime
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //1.数据源  虽然数据来自文本，但也是一条一条的读的
    val properties = new Properties
    properties.put("bootstrap.servers", "hadoop2:9092")
    properties.put("group.id", "test")
    properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    val dataSteam: DataStream[UserBehavior] = env.addSource(new FlinkKafkaConsumer[String]("topItems", new SimpleStringSchema(), properties))
      //    val dataSteam: DataStream[UserBehavior] = env.readTextFile("D:\\IDEA\\UserBehaviorAnalysis\\HotItemAnalysis\\src\\main\\resources\\UserBehavior.csv")
      .map {
        data => {
          val arr = data.split(",")
          UserBehavior(arr(0).trim.toLong, arr(1).trim.toLong, arr(2).trim.toInt, arr(3).trim, arr(4).trim.toLong)
        }
      } //将数据源格式化为样例类

//      .assignAscendingTimestamps(_.timeStamp * 1000L) //设置EventTime字段,数据源是严格递增的

      //BoundedOutOfOrdernessTimestampExtractor<T> implements AssignerWithPeriodicWatermarks<T>
      //public BoundedOutOfOrdernessTimestampExtractor(Time maxOutOfOrderness)
      //延时1秒 默认周期是 200 毫秒        如果数据是乱序的（有一定延迟）
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[UserBehavior](Time.milliseconds(1000)) {
        override def extractTimestamp(element: UserBehavior): Long = {
          element.timeStamp * 1000  //设置eventTime
        }
      })


    //2.处理数据
    val processSteam = dataSteam
      .filter(_.behavior == "pv") //只考虑浏览的情况

      //1).先聚合计算出每个商品的点击量 得到ItemViewCount
      .keyBy(_.itemId) //key是tuple或某个类型
      .timeWindow(Time.hours(1), Time.minutes(5)) //滑动窗口，窗口1小时，步长5分钟
      .aggregate(new CountAgg, new WindowResult) //窗口聚合 返回DataStream

      //2).在按照点击量进行排序
      .keyBy(_.windowEnd) //再按窗口endtime聚合，为了排序
      .process(new TopNHotItems)

    processSteam.print()
    env.execute("hot items")
  }
}

/** 自定义预聚合函数
 *
 * < IN>  The type of the values that are aggregated (input values)
 * < ACC> The type of the accumulator (intermediate aggregate state).
 * < OUT> The type of the aggregated result
 */
class CountAgg() extends AggregateFunction[UserBehavior, Long, Long] {
  override def createAccumulator(): Long = 0L

  override def add(value: UserBehavior, accumulator: Long): Long = accumulator + 1

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

/**
 * IN The type of the input value.
 * OUT The type of the output value.
 * KEY The type of the key.
 */
//AggregateFunction的输出就是WindowFunction的输入
class WindowResult extends WindowFunction[Long, ItemViewCount, Long, TimeWindow] {
  //input是一个窗口中某个key的相关集合，默认trigger一个窗口一触发
  override def apply(key: Long, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
    out.collect(ItemViewCount(key, window.getEnd, input.iterator.next()))
  }
}

/**
 * *@param < K> Type of the key.
 * *@param < I> Type of the input elements.
 * *@param < O> Type of the output elements.
 */
class TopNHotItems extends KeyedProcessFunction[Long, ItemViewCount, String] {
  private var itemState: ListState[ItemViewCount] = _
  private val topSize = 3

  override def processElement(value: ItemViewCount, ctx: KeyedProcessFunction[Long, ItemViewCount, String]#Context, out: Collector[String]): Unit = {
    //把每条数据存入状态列表，排序
    //一个窗口中，每条数据的windowEnd都是一样的
    itemState.add(value)
    //每到一个窗口结束就去处理一下
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 1) //窗口结束的时候触发定时器
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    val listBuffer: ListBuffer[ItemViewCount] = new ListBuffer
    import scala.collection.JavaConversions._
    for (item <- itemState.get()) {
      listBuffer.add(item)
    }
    val sortItems = listBuffer.sortBy(_.count)(Ordering.Long.reverse).take(topSize)
    itemState.clear()

    //格式化输出
    val result: StringBuilder = new StringBuilder
    result.append("时间：").append(new Timestamp(sortItems(0).windowEnd)).append("\n")
    for (i <- sortItems.indices) {
      var currItemVimeCount = sortItems(i)
      result.append("NO.").append(i + 1).append("：")
        .append("\t商品ID：").append(currItemVimeCount.ItemId)
        .append("\t浏览量：").append(currItemVimeCount.count)
        .append("\n")
    }
    result.append("=============================================================")
    out.collect(result.toString())
    Thread.sleep(1000)
  }

  override def open(parameters: Configuration): Unit = {
    itemState = getRuntimeContext.getListState(new ListStateDescriptor[ItemViewCount]("itemState", classOf[ItemViewCount]))
  }
}