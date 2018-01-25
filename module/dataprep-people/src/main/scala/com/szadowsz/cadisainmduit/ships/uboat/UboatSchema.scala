package com.szadowsz.cadisainmduit.ships.uboat

import org.apache.spark.sql.types._

/**
  * Created on 18/04/2016.
  */
object UboatSchema {

  val navyMap = Map(
    "The Royal Navy" -> "RN",
    "The US Navy" -> "USN",
    "The Royal Canadian Navy" -> "RCN",
    "The Soviet Navy" -> "VMF",
    "The French Navy" -> "FN",
    "The Royal Indian Navy" -> "RIN",
    "The Royal Australian Navy" -> "RAN",
    "The Royal Dutch Navy" -> "RDN",
    "The Italian Navy" -> "IN",
    "The Free French Navy" -> "FN",
    "The United States Coast Guard" -> "USN",
    "The Royal Norwegian Navy" -> "RNON",
    "The Royal Hellenic Navy" -> "HN",
    "The Royal New Zealand Navy" -> "RNZN",
    "The Brazilian Navy" -> "BN",
    "The Polish Navy" -> "PN",
    "The South African Navy" -> "RSAN"
  )

  val alliesMap = Map(
    "The Royal Navy" -> "Commonwealth",
    "The US Navy" -> "Usa",
    "The Royal Canadian Navy" -> "Commonwealth",
    "The Soviet Navy" -> "Other",
    "The French Navy" -> "Other",
    "The Royal Indian Navy" -> "Commonwealth",
    "The Royal Australian Navy" -> "Commonwealth",
    "The Royal Dutch Navy" -> "Other",
    "The Italian Navy" -> "Other",
    "The Free French Navy" -> "Other",
    "The United States Coast Guard" -> "Usa",
    "The Royal Norwegian Navy" -> "Other",
    "The Royal Hellenic Navy" -> "Other",
    "The Royal New Zealand Navy" -> "Commonwealth",
    "The Brazilian Navy" -> "Other",
    "The Polish Navy" -> "Other",
    "The South African Navy" -> "Commonwealth"
  )

  val classSchema = Array(
    "className",
    "built",
    "planned",
    "classType",
    "displacement",
    "length",
    "complement",
    "armament",
    "maxSpeed",
    "engines",
    "power",
    "classNotes",
    "classUrl"
    //  val classSchemaFields = Array(
    //    StructField("name", StringType),
    //    StructField("built", IntegerType),
    //    StructField("planned", IntegerType),
    //    StructField("type", StringType),
    //    StructField("displacement", StringType),
    //    StructField("length", StringType),
    //    StructField("complement", StringType),
    //    StructField("armament", StringType),
    //    StructField("maxSpeed", StringType),
    //    StructField("engines", StringType),
    //    StructField("power", StringType),
    //    StructField("classNotes", StringType)
  )

  val shipSchema = Array(
    "name",
    "url",
    "type",
    "navy",
    "class",
    "classUrl",
    "pennant",
    "builtBy",
    "laidDown",
    "launched",
    "commissioned",
    "endService",
    "history",
    "formerName",
    "lost",
    "careerNotes"
    //  val shipSchemaFields = Array(
    //    StructField("name", StringType),
    //    StructField("url", StringType),
    //    StructField("type", StringType),
    //    StructField("navy", StringType),
    //    StructField("class", StringType),
    //    StructField("classUrl", StringType),
    //    StructField("pennant", StringType),
    //    StructField("builtBy", StringType),
    //    StructField("laidDown", StringType),
    //    StructField("launched", StringType),
    //    StructField("commissioned", StringType),
    //    StructField("endService", StringType),
    //    StructField("history", StringType),
    //    StructField("formerName", StringType),
    //    StructField("lost", StringType),
    //    StructField("careerNotes", StringType)
  )


  //  val classSchema: StructType = StructType(classSchemaFields)
  //
  //  val shipSchema: StructType = StructType(shipSchemaFields)
}
