package com.szadowsz.grainne.census.io

import java.io.{File, FilenameFilter}

import akka.actor
import akka.actor.TypedActor.PostStop
import akka.actor.{ActorRef, Actor, PoisonPill}
import com.szadowsz.grainne.util.FileFinder
import org.slf4j.LoggerFactory

import scala.io.Source

/**
 * @author Zakski : 16/09/2015.
 */
class FileInputActor(directory: String, encoding: String, filter: FilenameFilter) extends Actor with PostStop{
  private val _logger = LoggerFactory.getLogger(classOf[FileInputActor])

  val dir = directory

  var writer : ActorRef = null

  var count:Int = 0

  protected var files: List[File] = FileFinder.search(directory, filter).toList

  protected var lineBuffer: Iterator[String] = createBuffer()

  protected def createBuffer(): Iterator[String] = {
    if (files.nonEmpty) {
      val head = files.head
      files = files.tail
      Source.fromFile(head, encoding).getLines()
    } else {
      Iterator.empty
    }
  }

  /**
   * Method reads a single line from the file.
   *
   * @return String if a line is available or returns None if EOF
   */
  protected def getLine: Option[String] = {
    if (lineBuffer.hasNext) {
      count += 1
      Some(lineBuffer.next())
    } else if (files.nonEmpty) {
      lineBuffer = createBuffer()
      getLine
    } else {
      self ! PoisonPill
      None
    }
  }

  def receive = {
    case actor : ActorRef =>
      writer = actor
      _logger.info("Acknowledged writer {}",writer)
    case "LineRequest" =>
      sender ! getLine
      _logger.debug("Parsed Line {} for {}",count,sender)
    case PoisonPill =>
      _logger.info("Closing File Input in {}",self)
      context.stop(self)
  }

  override def postStop() = {
    println(count)
    _logger.info("Messaging writer {} to stand down",writer)
    writer ! PoisonPill
  }
}

