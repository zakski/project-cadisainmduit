package com.szadowsz.grainne.census.mapping.origins

import java.io.{File, FileReader}
import java.util

import com.szadowsz.grainne.util.FileFinder
import org.supercsv.io.CsvListReader
import org.supercsv.prefs.CsvPreference

/**
  * Created by zakski on 15/11/2015.
  */
object FirstnameOrigins extends Origins{

  override def init() : Unit = {
    origins = loadOrigins("./data/firstnames/")
  }

  override def loadOrigins(directory: String) = {
    val files: List[File] = FileFinder.search(directory).toList
    var origins = Map[String, Set[String]]()

    files.foreach(file => {
      val csvReader = new CsvListReader(new FileReader(file), CsvPreference.STANDARD_PREFERENCE)

      var fields: util.List[String] = null
      do {
        fields = csvReader.read()
        if (fields != null) {
          val key = fields.get(0).toUpperCase
          val set = fields.get(2).split("\\|").toSet
          origins = origins.get(key) match {
            case Some(old) => origins + (key -> old.union(set))
            case None => origins + (key -> set)
          }
        }
      } while (fields != null)
    })

    origins
  }
}
