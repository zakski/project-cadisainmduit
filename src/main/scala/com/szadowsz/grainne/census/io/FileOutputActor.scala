package com.szadowsz.grainne.census.io

import java.io.FileWriter

import akka.actor.{PoisonPill, Actor}
import akka.actor.TypedActor.PostStop
import com.szadowsz.grainne.census.CensusDataBean
import com.szadowsz.grainne.census.mapping.origins.FirstnameOrigins
import com.szadowsz.grainne.census.supercsv.write.{OutputCell, StringCell, AgeBandingCell, StringSeqCell}
import org.supercsv.cellprocessor.ift.CellProcessor
import org.supercsv.io.CsvBeanWriter
import org.supercsv.prefs.CsvPreference

object FileOutputActor {
  private val headers1901: Array[String] = Array(
    "forename",
    //  "forenameOrigins",
    "middlenames",
    "surname",
    //  "surnameOrigins",
    "gender",
    "ageBanding",
    "county",
    "knowsIrish",
    "occupation",
    "religion"
    //    "literacy",
    //   "relationToHeadOfHouse",
    //   "married",
    //   "illnesses"
  )

  val cells1901: Array[CellProcessor] = Array(
    new StringCell,
    //  new StringSeqCell,
    new StringSeqCell,
    new StringCell,
    // new StringSeqCell,
    new OutputCell,
    new AgeBandingCell,
    new OutputCell,
    new StringSeqCell,
    new StringCell,
    new StringCell
  )
}


/**
  * Created by zakski on 11/11/2015.
  */
class FileOutputActor extends Actor with PostStop {

  val name = "./data/results/censusResults-"

  var count = 0
  var index = 0

  var csvWriter = new CsvBeanWriter(new FileWriter(name + count + ".csv"), CsvPreference.STANDARD_PREFERENCE)

  def receive = {
    case bean: CensusDataBean =>
      this.synchronized {
        csvWriter.write(bean, FileOutputActor.headers1901, FileOutputActor.cells1901)
        index += 1
        if (index == 100000) {
          index = 0
          count += 1
          csvWriter.close()
          csvWriter = new CsvBeanWriter(new FileWriter(name + count + ".csv"), CsvPreference.STANDARD_PREFERENCE)
        }
      }
    case PoisonPill => context.stop(self)
  }

  override def postStop() = {
    csvWriter.close()
    println(100000 * count + index)
    context.system.terminate()
  }
}
