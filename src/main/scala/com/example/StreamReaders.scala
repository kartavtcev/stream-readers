package com.example


import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props}
import akka.pattern.ask
import akka.stream.scaladsl.Source

import scala.collection.immutable.Queue
import scala.concurrent.duration._
import scala.util.Random
import java.time._
import java.time.temporal.ChronoUnit


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
  case class ReadStream(tick: LocalDateTime) extends Message
}

sealed trait Status
object Success extends Status
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

    case WorkerInfo(elapsedMs, bytesCnt, status) => ()
  }
}

object Worker {
  def props(dispatcherRef : ActorRef): Props = Props(new Worker(dispatcherRef))
  final case class Greeting(greeting: String)
}

class Worker(val dispatcherRef: ActorRef) extends Actor with ActorLogging {

  import Worker._
  var numberOfBytesRead : Long = 0
  var finished : Boolean = false
  var started: Option[LocalDateTime] = None

  def receive = {
    case Protocol.ReadStream(tick) =>

      started match {
        case None => started = Some(tick)
        case Some(_) => ()
      }

      if(!finished) {
        (dispatcherRef ? Protocol.GetStreamChunk) foreach { chunk =>
          val str = chunk.toString()
          numberOfBytesRead += str.length()
          if (str.contains(Generator.keyword)) {
            finished = true
            dispatcherRef ! WorkerInfo(Some(ChronoUnit.MILLIS.between(started.get, tick)), Some(numberOfBytesRead), Success)

          }
        }
      }

    case Timeout => ()


    case Greeting(greeting) =>
      log.info("Greeting received (from " + sender() + "): " + greeting)

  }
}

object StreamReaders extends App {

  val system: ActorSystem = ActorSystem("stream-readers-akka")
  val defaultTimeout = 60 seconds


  val dispatch: ActorRef =
    system.actorOf(Dispatcher.props(Generator.keyword), "dispatcherActor")

  dispatch ! Protocol.SetKeyword(Generator.keyword)

  //val randomStream =
  Source
    .tick(0.millis, 10.millis, "")
    .map {_ => Generator.randomStream }
    .async
    .runForeach {
      dispatch ! Protocol.StreamChunk(_)
    }

  import system.dispatcher
  //var cancelables : List[akka.actor.Cancellable] = List.empty

  for(i <- 1 to 10) {

    val worker: ActorRef = system.actorOf(Worker.props(dispatch), s"workerActor#$i")
    dispatch ! Protocol.RegisterWorker(worker)

    //val cancellable =
    system.scheduler.schedule(
        50 milliseconds,
        100 milliseconds,
        worker,
        Protocol.ReadStream(LocalDateTime.now))
    //cancelables = cancelables :+ cancellable

    system.scheduler.scheduleOnce(defaultTimeout) {
      worker !  Timeout(LocalDateTime.now)
    }
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

/*
object Buncher {
  trait Msg
  final case class Batch(messages: Vector[Msg])

  private case object TimerKey
  private case object Timeout extends Msg

  def behavior(target: ActorRef[Batch], after: FiniteDuration, maxSize: Int): Behavior[Msg] =
    Actor.withTimers(timers => idle(timers, target, after, maxSize))

  private def idle(timers: TimerScheduler[Msg], target: ActorRef[Batch],
                   after: FiniteDuration, maxSize: Int): Behavior[Msg] = {
    Actor.immutable[Msg] { (ctx, msg) =>
      timers.startSingleTimer(TimerKey, Timeout, after)
      active(Vector(msg), timers, target, after, maxSize)
    }
  }

  private def active(buffer: Vector[Msg], timers: TimerScheduler[Msg],
                     target: ActorRef[Batch], after: FiniteDuration, maxSize: Int): Behavior[Msg] = {
    Actor.immutable[Msg] { (ctx, msg) =>
      msg match {
        case Timeout =>
          target ! Batch(buffer)
          idle(timers, target, after, maxSize)
        case msg =>
          val newBuffer = buffer :+ msg
          if (newBuffer.size == maxSize) {
            timers.cancel(TimerKey)
            target ! Batch(newBuffer)
            idle(timers, target, after, maxSize)
          } else
            active(newBuffer, timers, target, after, maxSize)
      }
    }
  }
}
*/