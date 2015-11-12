package com.szadowsz.census.io

import java.io.FileWriter

import akka.actor.{PoisonPill, Actor}
import akka.actor.TypedActor.PostStop
import com.szadowsz.census.CensusDataBean
import com.szadowsz.census.supercsv.{GenderCell, AgeCell}
import org.supercsv.cellprocessor.{Trim, Optional}
import org.supercsv.cellprocessor.ift.CellProcessor
import org.supercsv.io.CsvBeanWriter
import org.supercsv.prefs.CsvPreference

object FileOutputActor {
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

  val cells1901: Array[CellProcessor] = Array(
    new Optional(new Trim()),
    new Optional(new Trim()),
    new Optional(new Trim()),
    new Optional(new Trim()),
    new Optional(new Trim()),
    new Optional(new Trim()),
    new Optional(new Trim()),
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
  * Created by zakski on 11/11/2015.
  */
class FileOutputActor extends Actor with PostStop{

  val name = "./data/results/censusResults-"

  var count = 0
  var index = 0

  var csvWriter = new CsvBeanWriter(new FileWriter(name + count + ".csv"), CsvPreference.STANDARD_PREFERENCE)

  var output = List[CensusDataBean]()

  def receive = {
     case bean : CensusDataBean =>
       index += 1
       csvWriter.write(bean,FileOutputActor.headers1901,FileOutputActor.cells1901)
       if (index == 100000) {
         index = 0
         count += 1
         csvWriter = new CsvBeanWriter(new FileWriter(name + count + ".csv"), CsvPreference.STANDARD_PREFERENCE)
       }
    case PoisonPill =>  context.stop(self)
  }

  override def postStop() = {
    println(output.length)
    context.system.terminate()
  }
}
