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
  case class StreamChunk(stream : Stream[Char])
}


object Dispatcher {

  def props(message: String, printerActor: ActorRef): Props = Props(new Dispatcher(message, printerActor))
  //final case class WhoToGreet(who: String)
  //case object Greet
}

class Dispatcher(message: String, printerActor: ActorRef) extends Actor {

  var queue : Queue[Char] = scala.collection.immutable.Queue.empty

  //var greeting = ""

  def receive = {
    //case WhoToGreet(who) =>
      //greeting = message + ", " + who
    //case Greet           =>
      //printerActor ! Greeting(greeting)
    case Protocol.StreamChunk(stream) =>

  }
}

object Printer {
  def props: Props = Props[Printer]
  final case class Greeting(greeting: String)
}

class Printer extends Actor with ActorLogging {
  import Printer._

  def receive = {
    case Greeting(greeting) =>
      log.info("Greeting received (from " + sender() + "): " + greeting)
  }
}

object AkkaQuickstart extends App {

  val randomStream = Source
    .tick(0.millis, 10.millis, "")
    .map {_ => Generator.randomStream }
    .async
    .runForeach {
      dispatch ! _
    }

  // Create the 'helloAkka' actor system
  val system: ActorSystem = ActorSystem("helloAkka")

  // Create the printer actor
  val printer: ActorRef = system.actorOf(Printer.props, "printerActor")

  // Create the 'greeter' actors
  val dispatch: ActorRef =
    system.actorOf(Dispatcher.props("Howdy", printer), "howdyGreeter")

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