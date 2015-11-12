package com.szadowsz.census.mapping

import java.io.{File, FileReader}
import java.util

import com.szadowsz.util.FileFinder
import org.supercsv.io.CsvListReader
import org.supercsv.prefs.CsvPreference

object SurnameOrigins {

  private var origins: Map[String, Set[String]] = null

  private def loadSurnameOrigins(directory: String) = {
    val files: List[File] = FileFinder.search(directory).toList
    var origins = Map[String, Set[String]]()

    files.foreach(file => {
      val csvReader = new CsvListReader(new FileReader(file), CsvPreference.STANDARD_PREFERENCE)

      var fields: util.List[String] = null
      do {
        fields = csvReader.read()
        if (fields != null) {
          val key = fields.get(0).toUpperCase
          val set = fields.get(1).split("\\|").toSet
          origins = origins.get(key) match {
            case Some(old) => origins + (key -> old.union(set))
            case None => origins + (key -> set)
          }
        }
      } while (fields != null)
    })
    origins
  }

  def init = origins = loadSurnameOrigins("./data/surnames/")

  def getOrigins(surname: String): Option[Set[String]] = {
    origins.get(surname.toUpperCase)
  }
}
