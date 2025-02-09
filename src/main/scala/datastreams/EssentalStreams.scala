package datastreams
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

  def main(args: Array[String]): Unit = {}
  demoTransformation()
}
