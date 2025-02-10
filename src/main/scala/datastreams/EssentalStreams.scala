package datastreams
import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.scala._ // import TypeInformation for the data of your DataStreams

object EssentalStreams {

  def applicationTemplate(): Unit = {
    // To start a Flink Job you need an execution environment
    val env: StreamExecutionEnvironment =
      StreamExecutionEnvironment.getExecutionEnvironment

    //In between you need to write the computation that you want

    val simpleNumberStream: DataStream[Int] = env.fromElements(1, 2, 3, 4, 5)

    //Perform some actions
    simpleNumberStream.print()
    // In the end you need to execute that environment
    env.execute() // trigger all the computations described earlier.
  }

  //transformations
  def demoTransformation(): Unit = {
    val env: StreamExecutionEnvironment =
      StreamExecutionEnvironment.getExecutionEnvironment

    val numbers: DataStream[Int] =
      env.fromElements(1, 2, 3, 4, 5)

    //set different parallelism
    env.setParallelism(2)

    //checking parallelism
    println(s"Current parallelism:${env.getParallelism}")

    //map
    val doubleNumbers: DataStream[Int] = numbers.map(_ * 2)

    //flatmap
    val expandedNumbers: DataStream[Int] = numbers.flatMap(n => List(n, n + 1))

    //filter

    val filteredNumbers: DataStream[Int] =
      numbers.filter(_ % 2 == 0).setParallelism(4)

    val finalData = expandedNumbers.writeAsText("output/expandedStream")
    finalData.setParallelism(3)
    env.execute()
  }

  /*Exercise: FizzBuzz on Flink*/

  case class FizzBuzzResult(n: Long, output: String)

  def fizzBuzzExercise(): Unit = {
    val env: StreamExecutionEnvironment =
      StreamExecutionEnvironment.getExecutionEnvironment

    val numbers = env.fromSequence(1, 30)

    val fizzbuzz = numbers
      .map[FizzBuzzResult] { (x: Long) =>
        x match {
          case n if n % 3 == 0 && n % 5 == 0 => FizzBuzzResult(n, "fizzbuzz")
          case n if n % 3 == 0               => FizzBuzzResult(n, "fizz")
          case n if n % 5 == 0               => FizzBuzzResult(n, "buzz")
          case n                             => FizzBuzzResult(n, s"${n}")
        }
      }
      .filter(_.output == "fizzbuzz")
      .map(_.n)
    fizzbuzz
      .addSink(
        StreamingFileSink
          .forRowFormat(
            new Path("output/streaming_sink"),
            new SimpleStringEncoder[Long]("UTF-8")
          )
          .build()
      )
      .setParallelism(1)

    env.execute()
  }

  //explicit transformations

  def demoExplicitTransformations(): Unit = {
    val env: StreamExecutionEnvironment =
      StreamExecutionEnvironment.getExecutionEnvironment

    val numbers = env.fromSequence(1, 30)

    //map
    val doubleNumbers = numbers.map(_ * 2)
  }

  def main(args: Array[String]): Unit = {}
  fizzBuzzExercise()
}
