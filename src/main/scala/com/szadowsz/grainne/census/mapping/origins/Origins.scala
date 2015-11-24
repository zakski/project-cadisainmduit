package com.szadowsz.grainne.census.mapping.origins

import java.io.{FileReader, File}
import java.util

import com.szadowsz.grainne.util.FileFinder
import org.supercsv.io.CsvListReader
import org.supercsv.prefs.CsvPreference

/**
  * Created by zakski on 18/11/2015.
  */
private[origins] trait Origins {

  protected var origins: Map[String, Set[String]] = null

  def init() : Unit

  def loadOrigins(directory: String): Map[String, Set[String]]

  def getOrigins(forename: String): Option[Set[String]] = {
    origins.get(forename.toUpperCase)
  }
}
