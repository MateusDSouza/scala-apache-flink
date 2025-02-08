package scalarecap

import java.util.concurrent.Executors
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

object ScalaRecap extends App {

  //expressions
  val anIfExpression: String = if (2 > 3) "bigger" else "smaller"

  //instructions vs expressions

  val theUnit = println("uasasuasduiasdui")

  class Animal
  class Cat extends Animal
  trait Carnivore {
    def eat(animal: Animal): Unit
  }

  //inheritance: extends <=1, but inherits more than 1 trait
  class Crocodile extends Animal with Carnivore {
    override def eat(animal: Animal): Unit = println("eat meat")
  }

  //singleton

  object Singleton

  //companions

  object Carnivore

  case class Person(name: String, age: Int)

  //Generics
  class MyList[A]

  //method notation - syntactic sugar
  val three = 1 + 2
  val _3 = 1.+(2)

  //FP

  val incrementer: Int => Int = x => x + 1
  val incremented = incrementer(5)

  //map, flatMap, filter = HOFs
  val processedList = List(1, 2, 3).map(incrementer) //[2,3,4]
  val aLongerList = List(1, 2, 3).flatMap(x => List(x, x + 1)) //[1,2,2,3,3,4]

  //for-comprehensions

  val checkerboard =
    List(1, 2, 3).flatMap(n => List('a', 'b', 'c').map(c => (n, c)))

  val checkerboardEasy = for {
    n <- List(1, 2, 3)
    c <- List('a', 'b', 'c')
  } yield (n, c) //same

  //options and try

  val anOption: Option[Int] = Option( /*this can be null*/ 43)
  val doubleOption = anOption.map(_ * 2)

  val anAttempt: Try[Int] = Try(12)

  val modifiedAttempt = anAttempt.map(_ * 10)

  // pattern matching
  val anUnknown: Any = 45

  val medal: String = anUnknown match {
    case 1 => "gold"
    case 2 => "silver"
    case 3 => "bronze"
    case _ => "no medal"
  }

  val optionDescription = anOption match {
    case Some(value) => s"THE OPTION IS NOT EMPTY: $value"
    case None        => "the option is empty"
  }

  //Futures
  val ec: ExecutionContext =
    ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(8))
  val aFuture =
    Future( /*something to be evaluated on another thread*/ 1 + 999)(ec)

  //implicits

  // 1 - implicit argument of values
  implicit val timeout: Int = 3000
  def setTimeout(f: () => Unit)(implicit tout: Int) = {
    Thread.sleep {
      tout
    }
    f()
  }

  setTimeout(() => println("timeout"))

  //2 - extension methods

}
