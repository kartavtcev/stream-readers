package com.example

import java.time._
import java.time.temporal.ChronoUnit

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props}
import akka.pattern.ask
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Source
import akka.util.Timeout

import scala.collection.immutable.Queue
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.util.Random


object Generator {
  val keyword = "Lpfn"

  def randomStream : Stream[Char] = {
    def stream = Random.alphanumeric take Random.nextInt(100)
    if(Random.nextInt(1000) == 1) stream #::: keyword.toStream
    else stream
  }
}

object Protocol {
  sealed trait Message
  case class StreamChunk(stream : Stream[Char]) extends Message
  object GetStreamChunk extends Message
  case class SetKeyword(keyword: String) extends Message
  case class RegisterWorker(ref: ActorRef) extends Message
  object ReadStream extends Message

  sealed trait Status
  object Success extends Status {
    override def toString: String = "Success"
  }
  object Timeout extends Status {
    override def toString: String = "Timeout"
  }
  case class Failure(exceptionMsg: String) extends Status {
    override def toString: String = "Failure"
  }

  case class WorkerInfo(selfRef : ActorRef, elapsedMs: Option[Long], bytesCnt: Option[Long], status: Status) extends Message
}


object Dispatcher {

  def props(keyword: String): Props = Props(new Dispatcher(keyword))
}

class Dispatcher(var keyword: String) extends Actor with ActorLogging {

  var workers = Map.empty[ActorRef, Option[Protocol.WorkerInfo]]
  var queue: Queue[Char] = scala.collection.immutable.Queue.empty
  var isTimeout = false

  def receive = {
    case Protocol.StreamChunk(stream) =>
      if(!isTimeout) {
        stream.foreach { c => queue = queue.enqueue(c) }
      }

    case Protocol.SetKeyword(word) =>
      log.debug("dispatcher got keyword")
      keyword = word

    case Protocol.GetStreamChunk =>
      if (queue.length > (keyword.length - 1)) { // FIXED: the keyword was on the boundary & 1st worker got one part of the word, & 2nd worker got another part of the word

        val qs =  queue.toStream
        log.debug(s"dispatcher sent-out a stream-chunk ${qs.mkString}")
        sender() ! Protocol.StreamChunk(qs)
        queue = queue.take(keyword.length - 1)

      } else {
        log.debug(s"dispatcher sent-out empty stream-chunk")
        sender() ! Protocol.StreamChunk(Stream.empty)
      }

    case Protocol.RegisterWorker(ref) =>
      workers += (ref -> None)

    case wi : Protocol.WorkerInfo =>
      log.debug(s"dispatcher got worker-info:\n$wi")
      workers -= wi.selfRef
      workers += wi.selfRef -> Some(wi)

    case Protocol.Timeout =>
      isTimeout = true
      log.debug("dispatcher got timeout")

      val workersSuccess = workers.values
        .collect{ case Some(wi) => wi }
        .collect{ case wi @ Protocol.WorkerInfo(_, Some(_), Some(_), _) => wi }
        .toList
        .sortBy((- _.elapsedMs.get))

      val workersFail = workers.values
        .collect{ case Some(wi) => wi }
        .collect{ case wi @ Protocol.WorkerInfo(_, None, None, _) => wi }
        .toList

      workersSuccess.foreach { wi =>
        log.info(s"${wi.elapsedMs.get} ${wi.bytesCnt.get} ${wi.status} ${wi.selfRef}")
      }

      workersFail.foreach { wi =>
        log.info(s"${wi.status} ${wi.selfRef}")
      }

      val stat = workersSuccess.foldLeft((0L,0L))((acc, wi) => (acc._1 + wi.elapsedMs.get, acc._2 + wi.bytesCnt.get))
      log.info(s"Average bytes read per second: ${(stat._2.toFloat / stat._1.toFloat) * 1000}\n\n")
  }
}

object Worker {
  def props(dispatcherRef : ActorRef): Props = Props(new Worker(dispatcherRef))
}

class Worker(val dispatcherRef: ActorRef) extends Actor with ActorLogging {

  implicit val timeout: Timeout = Timeout(1 seconds)

  var numberOfBytesRead : Long = 0
  var isFinished = false
  var started: Option[LocalDateTime] = None

  def receive = {
    case Protocol.ReadStream =>

      started match {
        case None => started = Some(LocalDateTime.now)
        case Some(_) => ()
      }

      if(!isFinished) {
        (dispatcherRef ? Protocol.GetStreamChunk) map { case Protocol.StreamChunk (chunk) =>
          val str = chunk.mkString
          log.debug(s"worker got a chunk of stream: $str")
          numberOfBytesRead += str.length()
          if (str.contains(Generator.keyword)) {
            isFinished = true
            dispatcherRef ! Protocol.WorkerInfo(self, Some(ChronoUnit.MILLIS.between(started.get, LocalDateTime.now)), Some(numberOfBytesRead), Protocol.Success)
          }
        }
      }

    case Protocol.Timeout =>
      if(!isFinished) {
        isFinished = true
        log.debug("worker got timeout")
        dispatcherRef ! Protocol.WorkerInfo(self, None, None, Protocol.Timeout)
      }
  }
}

object StreamReaders extends App {    // TODO: Error handling

  implicit val system: ActorSystem = ActorSystem("stream-readers-akka")
  implicit val materializer: ActorMaterializer = ActorMaterializer()

  val defaultWorkerTimeout = 60 seconds
  val defaultDispatcherTimeout = 70 seconds
  val terminateActorSystemTimeout = 80 seconds

  // TODO: Enter timeout from the console

  val dispatch: ActorRef =
    system.actorOf(Dispatcher.props(Generator.keyword), "dispatcherActor")

  dispatch ! Protocol.SetKeyword(Generator.keyword)

  Source
    .tick(0.millis, 10.millis, "")
    .map {_ => Generator.randomStream }
    .async
    .runForeach {
      dispatch ! Protocol.StreamChunk(_)
    }

  for(i <- 1 to 10) {

    val worker: ActorRef = system.actorOf(Worker.props(dispatch), s"workerActor-number-$i")
    dispatch ! Protocol.RegisterWorker(worker)

    system.scheduler.schedule(
        0 milliseconds,
        100 milliseconds,
        worker,
        Protocol.ReadStream)

    system.scheduler.scheduleOnce(defaultWorkerTimeout) {
      worker !  Protocol.Timeout
    }
  }

  system.scheduler.scheduleOnce(defaultDispatcherTimeout) {
    dispatch ! Protocol.Timeout
  }


  system.scheduler.scheduleOnce(terminateActorSystemTimeout) {
    system.terminate()
  }
}