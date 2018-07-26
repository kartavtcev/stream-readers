package com.example


import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props}
import akka.stream.scaladsl.Source

import scala.collection.immutable.Queue
import scala.concurrent.duration._
import scala.util.Random

object Generator {
  val keyword = "Lpfn"

  def randomStream : Stream[Char] = {
    def stream = Random.alphanumeric take Random.nextInt(100)
    if(Random.nextInt(100) < 1) stream #::: keyword.toStream
    else stream
  }
}

object Protocol {
  sealed trait Message
  case class StreamChunk(stream : Stream[Char]) extends Message
  object GetStreamChunk extends Message
  case class SetKeyword(keyword: String) extends Message
  case class RegisterWorker(ref: ActorRef) extends Message
  object Start extends Message
}

sealed trait Status
case class Success(bytes: Long) extends Status
object Timeout extends Status
case class Failure(exceptionMsg: String) extends Status

case class WorkerInfo(elapsedMs: Option[Long], bytesCnt: Option[Long], status: Status)


object Dispatcher {

  def props(keyword: String): Props = Props(new Dispatcher(keyword))
  //final case class WhoToGreet(who: String)
  //case object Greet
}

class Dispatcher(var keyword: String) extends Actor with ActorLogging {

  var workers = Map.empty[ActorRef, Option[WorkerInfo]]
  var queue: Queue[Char] = scala.collection.immutable.Queue.empty

  def receive = {
    case Protocol.StreamChunk(stream) =>
      stream.foreach { c => queue = queue.enqueue(c) }

    case Protocol.SetKeyword(word) =>
      keyword = word

    case Protocol.GetStreamChunk =>
      if (queue.length > (keyword.length - 1)) { // FIX: the keyword was on the boundary & 1st worker got one part of the word, & 2nd worker got another part of the word

        sender() ! Protocol.StreamChunk(queue.toStream)
        queue = queue.take(keyword.length - 1)

      } else {
        sender() ! Protocol.StreamChunk(Stream.empty)
      }

    case Protocol.RegisterWorker(ref) =>
      workers += (ref -> None)
  }
}

object Worker {
  def props: Props = Props[Worker]
  final case class Greeting(greeting: String)
}

class Worker extends Actor with ActorLogging {

  import Worker._

  def receive = {
    case Greeting(greeting) =>
      log.info("Greeting received (from " + sender() + "): " + greeting)
  }
}

object AkkaQuickstart extends App {

  val system: ActorSystem = ActorSystem("stream-readers-akka")

  val dispatch: ActorRef =
    system.actorOf(Dispatcher.props(Generator.keyword), "dispatcherActor")

  dispatch ! Protocol.SetKeyword(Generator.keyword)

  for(i <- 1 to 10) {
    val worker: ActorRef = system.actorOf(Worker.props, s"workerActor#$i")
    dispatch ! Protocol.RegisterWorker(worker)
  }

  val randomStream = Source
    .tick(0.millis, 10.millis, "")
    .map {_ => Generator.randomStream }
    .async
    .runForeach {
      dispatch ! _
    }



  /*
  val helloGreeter: ActorRef =
    system.actorOf(Greeter.props("Hello", printer), "helloGreeter")
  val goodDayGreeter: ActorRef =
    system.actorOf(Greeter.props("Good day", printer), "goodDayGreeter")

  howdyGreeter ! WhoToGreet("Akka")
  howdyGreeter ! Greet

  howdyGreeter ! WhoToGreet("Lightbend")
  howdyGreeter ! Greet

  helloGreeter ! WhoToGreet("Scala")
  helloGreeter ! Greet

  goodDayGreeter ! WhoToGreet("Play")
  goodDayGreeter ! Greet
  */
}