package com.szadowsz.census.akka.process

import akka.actor.{Actor, ActorRef}
import com.szadowsz.census.akka.io.LineRequest
import com.szadowsz.census.akka.process.cell.GenderCell
import org.supercsv.cellprocessor.ift.CellProcessor
import org.supercsv.cellprocessor.{Optional, ParseInt, Trim}
import org.supercsv.io.CsvBeanReader
import org.supercsv.prefs.CsvPreference

import java.io.StringReader

case object BeginProcessing

object CensusWorker {

  private val headers1901: Array[String] = Array(
    "surname",
    "forename",
    "townlandOrStreet",
    "ded",
    "county",
    "age",
    "gender",
    "birthplace",
    "occupation",
    "religion",
    "literacy",
    "knowsIrish",
    "relationToHeadOfHouse",
    "married",
    "illnesses")

  val cells1901: List[CellProcessor] = List(
    new Optional(new Trim()),
    new Optional(new Trim()),
    new Optional(new Trim()),
    new Optional(new Trim()),
    new Optional(new Trim()),
    new Optional(new ParseInt()),
    new GenderCell(),
    new Optional(new Trim()),
    new Optional(new Trim()),
    new Optional(new Trim()),
    new Optional(new Trim()),
    new Optional(new Trim()),
    new Optional(new Trim()),
    new Optional(new Trim()),
    new Optional(new Trim()))

}

/**
 * @author Zakski : 16/09/2015.
 */
class CensusWorker(reader: ActorRef) extends Actor {


  var count: Int = 0

  def process(line: String) = {
    val csvReader = new CsvBeanReader(new StringReader(line), CsvPreference.STANDARD_PREFERENCE)

    var bean: Census1901DataBean = csvReader.read(classOf[Census1901DataBean],
      CensusWorker.headers1901,
      CensusWorker.cells1901: _*)

    println(bean.toString)

    count += 1
  }

  def receive = {
    case BeginProcessing => reader ! LineRequest

    case Some(line: String) =>
      process(line)
      reader ! LineRequest

    case None =>
      println(count)
      context.stop(self)
  }
}
