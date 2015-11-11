package com.szadowsz.census.io

import java.io.{File, FilenameFilter}

import akka.actor.TypedActor.PostStop
import akka.actor.{Actor, PoisonPill}
import com.szadowsz.util.FileFinder

import scala.io.Source

case object LineRequest

/**
 * @author Zakski : 16/09/2015.
 */
class FileInputActor(directory: String, encoding: String, filter: FilenameFilter) extends Actor with PostStop{

  val dir = directory

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
    case LineRequest => sender ! getLine
    case PoisonPill => context.stop(self)
  }

  override def postStop() = {
    println(count)
    context.system.terminate()
  }
}

