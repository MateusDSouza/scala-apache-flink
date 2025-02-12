package datastreams

import generators.gaming._
import org.apache.flink.api.common.eventtime.{
  SerializableTimestampAssigner,
  WatermarkStrategy
}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.{
  AllWindowFunction,
  ProcessAllWindowFunction,
  ProcessWindowFunction,
  WindowFunction
}
import org.apache.flink.streaming.api.windowing.assigners.{
  SlidingProcessingTimeWindows,
  TumblingEventTimeWindows
}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

import java.lang
import java.time.Instant
import scala.concurrent.duration._

object WindowFunctions {

  //stream of events for a gaming session

  val env = StreamExecutionEnvironment.getExecutionEnvironment

  implicit val serverStartTime: Instant =
    Instant.parse("2022-02-02T00:00:00.000Z")
  val events: List[ServerEvent] = List(
    bob.register(2.seconds), //player bob registered 2s after the server started
    bob.online(2.seconds),
    sam.register(3.seconds),
    sam.online(4.seconds),
    rob.register(4.seconds),
    alice.register(4.seconds),
    mary.register(6.seconds),
    mary.online(6.seconds),
    carl.register(8.seconds),
    rob.online(10.seconds),
    alice.online(10.seconds),
    carl.online(10.seconds)
  )

  val eventStream: DataStream[ServerEvent] = env
    .fromCollection(events)
    .assignTimestampsAndWatermarks(
      WatermarkStrategy
        .forBoundedOutOfOrderness(
          java.time.Duration.ofMillis(500)
        ) //once get an event, you will not accept an event greater than 0.5 s from the watermark
        .withTimestampAssigner(new SerializableTimestampAssigner[ServerEvent] {
          override def extractTimestamp(
              element: ServerEvent,
              recordTimestamp: Long
          ): Long = element.eventTime.toEpochMilli
        })
    ) //how to extract timestamps for events (event time) + watermarks

  //how many players were registered every 3 seconds?
  //[0s..3s][3s..6s][6s..9s][9s..12s]

  val threeSecondsTumblingWindow =
    eventStream.windowAll(TumblingEventTimeWindows.of(Time.seconds(3)))

  //count by windowAll

  class CountByWindowAll
      extends AllWindowFunction[ServerEvent, String, TimeWindow] {
    override def apply(
        window: TimeWindow,
        input: Iterable[ServerEvent],
        out: Collector[String]
    ): Unit = {
      val registrationEventCount =
        input.count(event => event.isInstanceOf[PlayerRegistered])
      out.collect(
        s"Window [${window.getStart} - ${window.getEnd}] ${registrationEventCount}"
      )
    }
  }

  //alternative: use a process window function which offers a much richer API (lower level)
  class CountByWindowAllV2
      extends ProcessAllWindowFunction[ServerEvent, String, TimeWindow] {
    override def process(
        context: Context,
        elements: Iterable[ServerEvent],
        out: Collector[String]
    ): Unit = {
      val window = context.window
      val registrationEventCount =
        elements.count(event => event.isInstanceOf[PlayerRegistered])
      out.collect(
        s"Window [${window.getStart} - ${window.getEnd}] ${registrationEventCount}"
      )
    }
  }

  //alternative 2: aggregate function

  class CountByWindowAllV3 extends AggregateFunction[ServerEvent, Long, Long] {

    override def createAccumulator(): Long = 0L

    override def add(value: ServerEvent, accumulator: Long): Long =
      if (value.isInstanceOf[PlayerRegistered]) accumulator + 1
      else accumulator

    override def getResult(accumulator: Long): Long = accumulator

    override def merge(a: Long, b: Long): Long = a + b
  }

  def demoCountByWindow(): Unit = {
    val registrationsPerThreeSeconds: DataStream[String] =
      threeSecondsTumblingWindow.apply(new CountByWindowAll)
    registrationsPerThreeSeconds.print()
    env.execute()
  }

  def demoCountByWindow_v2(): Unit = {
    val registrationsPerThreeSeconds: DataStream[String] =
      threeSecondsTumblingWindow.process(new CountByWindowAllV2)
    registrationsPerThreeSeconds.print()
    env.execute()
  }

  def demoCountByWindow_v3(): Unit = {
    val registrationsPerThreeSeconds: DataStream[Long] =
      threeSecondsTumblingWindow.aggregate(new CountByWindowAllV3)
    registrationsPerThreeSeconds.print()
    env.execute()
  }

  /*
   * Keyed Streams
   * */

  //each element will be assigned to a "mini-stream" for its own key
  val streamByType: KeyedStream[ServerEvent, String] =
    eventStream.keyBy(e => e.getClass.getSimpleName)

  //for every key, we will have a separate window allocation
  val threeSecondsTumblingWindowsByType
      : WindowedStream[ServerEvent, String, TimeWindow] = streamByType.window {
    TumblingEventTimeWindows.of(Time.seconds(3))
  }

  class CountByWindow
      extends WindowFunction[ServerEvent, String, String, TimeWindow] {
    override def apply(
        key: String,
        window: TimeWindow,
        input: Iterable[ServerEvent],
        out: Collector[String]
    ): Unit =
      out.collect(s"$key, ${input.size}")
  }

  def demoCountByTypeByWindow(): Unit = {
    val finalStream = threeSecondsTumblingWindowsByType.apply(new CountByWindow)
    finalStream.print()
    env.execute()
  }

  class CountByWindowV2
      extends ProcessWindowFunction[ServerEvent, String, String, TimeWindow] {
    override def process(
        key: String,
        context: Context,
        elements: Iterable[ServerEvent],
        out: Collector[String]
    ): Unit =
      out.collect(s"$key: ${context.window} ${elements.size}")
  }

  def demoCountByTypeByWindowV2(): Unit = {
    val finalStream =
      threeSecondsTumblingWindowsByType.process(new CountByWindowV2)
    finalStream.print()
    env.execute()
  }

  /** Sliding Windows
    */

  //How many players were registered every 3 seconds, UPDATED EVERY 1s?
  // 0s-3s,1s-4s,2s-5s

  def demoSlidingWindows(): Unit = {
    val windowSize: Time = Time.seconds(3)
    val slidingTime: Time = Time.seconds(1)
    val slidingWindowsAll = eventStream.windowAll(
      SlidingProcessingTimeWindows.of(windowSize, slidingTime)
    )

    //process the windowed with similar window functions

    val registrationCountByWindow =
      slidingWindowsAll.apply(new CountByWindowAll)
  }

  /** Session windows = group of events with NO MORE THAN a certain time gap in between events
    */

  def main(args: Array[String]): Unit = {}
  demoCountByTypeByWindowV2()
}
