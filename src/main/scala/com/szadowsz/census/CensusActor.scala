package com.szadowsz.census

import java.io.StringReader
import akka.actor.{Actor, ActorRef}
import com.szadowsz.census.supercsv.{MissingCell, AgeCell, GenderCell}
import org.slf4j.LoggerFactory
import org.supercsv.cellprocessor.ift.CellProcessor
import org.supercsv.cellprocessor.{Optional, Trim}
import org.supercsv.io.CsvBeanReader
import org.supercsv.prefs.CsvPreference

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
    new MissingCell,
    new MissingCell,
    new MissingCell,
    new MissingCell,
    new MissingCell,
    new AgeCell(),
    new GenderCell(),
    new MissingCell,
    new MissingCell,
    new MissingCell,
    new MissingCell,
    new MissingCell,
    new MissingCell,
    new MissingCell,
    new MissingCell)

}

/**
 * @author Zakski : 16/09/2015.
 */
class CensusActor() extends Actor {
  private val _logger = LoggerFactory.getLogger(classOf[CensusActor])


  var reader : ActorRef = null
  var writer : ActorRef = null

  var count: Int = 0

  def process(line: String) = {
    val csvReader = new CsvBeanReader(new StringReader(line), CsvPreference.STANDARD_PREFERENCE)

    val bean: CensusDataBean = csvReader.read(classOf[CensusDataBean],CensusActor.headers1901,CensusActor.cells1901: _*)

    count += 1
    writer ! bean
  }

  def receive = {
    case (read : ActorRef, write : ActorRef) =>
      reader = read
      writer = write
      _logger.info("Initialised Census Actor with reader {} and writer {}",List(reader, writer):_*)
      reader ! "LineRequest"

    case Some(line: String) =>
      process(line)
      reader ! "LineRequest"

    case None =>
      _logger.info("Census Actor finished with count of {}",count)
      context.stop(self)
  }
}
