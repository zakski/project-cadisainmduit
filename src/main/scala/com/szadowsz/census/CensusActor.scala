package com.szadowsz.census

import java.io.StringReader
import akka.actor.{Actor, ActorRef}
import com.szadowsz.census.io.LineRequest
import com.szadowsz.census.supercsv.{AgeCell, GenderCell}
import org.supercsv.cellprocessor.ift.CellProcessor
import org.supercsv.cellprocessor.{Optional, ParseInt, Trim}
import org.supercsv.io.CsvBeanReader
import org.supercsv.prefs.CsvPreference

case object BeginProcessing

object CensusActor {

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
    new AgeCell(),
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
class CensusActor(val reader: ActorRef, val writer: ActorRef) extends Actor {


  var count: Int = 0

  def process(line: String) = {
    val csvReader = new CsvBeanReader(new StringReader(line), CsvPreference.STANDARD_PREFERENCE)

    var bean: CensusDataBean = csvReader.read(classOf[CensusDataBean],
      CensusActor.headers1901,
      CensusActor.cells1901: _*)


    count += 1
    writer ! bean
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
