package com.szadowsz.cadisainmduit.ships.wiki.us

import java.sql.Date
import java.text.SimpleDateFormat
import java.util.Calendar

import com.szadowsz.common.io.read.CsvReader
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, datediff, udf}

import scala.util.Try

/**
  * Created on 18/04/2016.
  */
object UsNavySchema {

  private[wiki] val classSchema = Array(
    "name",
    "fullname",
    "motto",
    "id",
    "yardNo",
    "wayNo",
    "classAndType",
    "type",
    "crew",
    "namesake",
    "nicknames",
    "renamed",
    "homeport",
    "builder",
    "ordered",
    "laidDown",
    "launched",
    "commissioned",
    "decommissioned",
    "fate",
    "armament",
    "aircraftCarried",
    "electronicWarfareAndDecoys",
    "boatsAndLandingCraftCarried",
    "aviationFacilities",
    "url_redirected",
    "url_fragment",
    "line_desc",
    "sunk",
    "planned",
    "scrapped",
    "speed",
    "propulsion",
    "armour",
    "relaunched",
    "deckClearance",
    "awarded",
    "reclassified",
    "length",
    "succeededBy",
    "displacement",
    "honoursAndAwards",
    "sailPlan",
    "notes",
    "completed",
    "outOfService",
    "lost",
    "building",
    "route",
    "sponsoredBy",
    "maidenVoyage",
    "partOf",
    "cost",
    "inCommission",
    "draught",
    "precededBy",
    "commanders",
    "owner",
    "built",
    "depth",
    "troops",
    "armor",
    "depthOfHold",
    "awards",
    "operator",
    "acquired",
    "testDepth",
    "portOfRegistry",
    "christened",
    "retired",
    "draft",
    "height",
    "operations",
    "converted",
    "struck",
    "preserved",
    "iceClass",
    "complement",
    "sensorsAndProcessingSystems",
    "honorsAndAwards",
    "captured",
    "cancelled",
    "active",
    "endurance",
    "victories",
    "tonnage",
    "reinstated",
    "tonsBurthen",
    "subclasses",
    "capacity",
    "decks",
    "operators",
    "range",
    "status",
    "badge",
    "refit",
    "inService",
    "beam",
    "builders",
    "installedPower",
    "recommissioned",
    "ramps"
  )

  private[wiki] val months = List(
    "January",
    "February",
    "March",
    "April",
    "May",
    "June",
    "July",
    "August",
    "September",
    "October",
    "November",
    "December"
  )

  private[wiki] val stopList = List(
    " transferred",
    "\\,",
    " purchased",
    " launched",
    "\\.",
    " in service",
    " originally",
    " transferred",
    " captured",
    " that",
    " bought",
    " commissioned",
    " sunk",
    " hired",
    " built",
    " listed",
    " dating",
    " which",
    " sold",
    " wrecked",
    " previously",
    " in use",
    " laid down",
    " acquired",
    " ordered",
    " under"
  )

  private[wiki] val linePat = "^.*?(?:was a|was an|\\, a|\\, an|was the|was to have been a) (.*?)(?:" + stopList.mkString("|") + ").*$"
  private[wiki] val datePat = "^.*((?:\\d{2} ){0,1}(?:" + months.mkString("|") + "){0,1} \\d{4}).*?$"

  val dateUdf: UserDefinedFunction = udf[Date, String]((s: String) => {
    val sDf = new SimpleDateFormat("dd MMM yyyy")
    val sDf2 = new SimpleDateFormat("MMM yyyy")
    val sDf3 = new SimpleDateFormat("yyyy")
    Try(sDf.parse(s)).orElse(Try(sDf2.parse(s))).orElse(Try(sDf3.parse(s))).map(d => new Date(d.getTime)).toOption.orNull
  })

  val dateComboUdf: UserDefinedFunction = udf[Date, Date, Date]((d1: Date, d2: Date) => if (d1 == null) d2 else d1)
  val strComboUdf: UserDefinedFunction = udf[String, String, String]((d1: String, d2: String) => if (d1 == null) d2 else d1)
  val strAppUdf: UserDefinedFunction = udf[String, String, String]((d1: String, d2: String) => if (d1 == null) d2 else if (d2 == null) d1 else d1 + " " + d2)

  def getYear(c: Option[Calendar]): Int = c.get.get(Calendar.YEAR)

  private[wiki] def shapeData(dfShip: DataFrame): DataFrame = {
    dfShip
      .withColumn("orderedDate", dateUdf(col("ordered")))
      .withColumn("laidDownDate", dateUdf(col("laidDown")))
      .withColumn("launchedDate", dateUdf(col("launched")))
      .withColumn("commissionedDate", dateUdf(col("commissioned")))
      .withColumn("decommissionedDate", dateUdf(col("decommissioned")))
      .withColumn("fateDate", dateUdf(col("fate")))
      .withColumn("descStartDate", dateUdf(col("descStart")))
      .withColumn("descEndDate", dateUdf(col("descEnd")))
      .withColumn("startDate", dateComboUdf(dateComboUdf(dateComboUdf(dateComboUdf(col("commissionedDate"), col("launchedDate")), col("laidDownDate")), col("orderedDate")), col("descStartDate")))
      .withColumn("endDate", dateComboUdf(dateComboUdf(col("decommissionedDate"), col("fateDate")), col("descEndDate")))
      .withColumn("classAndTypeDesc", strComboUdf(strComboUdf(col("classAndType"), col("desc")), col("type")))
      .withColumn("daysActive", datediff(col("endDate"), col("startDate")))
      .select("name", "classAndTypeDesc", "navy", "country", "startDate", "endDate", "daysActive")
  }

  private[wiki] def shapeFinalData(dfShip: DataFrame): DataFrame = {
    val typeData = new CsvReader("./data/web/uboat/uboatInfo.csv")
    val typeSet = typeData.readAll().filter(row => row(4) == "Usa").map(s => (s.head, s(1))).toSet

    val servedInWWII: UserDefinedFunction = udf[Boolean, String, String, Date, Date]((name: String, typ: String, d1: Date, d2: Date) => {
      val c1 = Option(d1).map(d => {
        val c = Calendar.getInstance()
        c.setTime(d)
        c
      })
      val c2 = Option(d1).map(d => {
        val c = Calendar.getInstance()
        c.setTime(d)
        c
      })
      !((c1.isDefined && getYear(c1) <= 1945 && getYear(c1) >= 1939) ||
        (c2.isDefined && getYear(c2) >= 1939 && getYear(c2) <= 1945) ||
        (c1.isDefined && c2.isDefined && getYear(c1) < 1939 && getYear(c2) > 1945)) ||
        !typeSet.contains((name, typ))
    })

    dfShip.filter(servedInWWII(col("name"), col("classAndTypeDesc"),col("startDate"), col("endDate")))
      .withColumn("class", strComboUdf(col("classDesc"),col("typeDesc")))
      .withColumn("type", strAppUdf(col("rateDesc"),col("classAndTypeDesc")))
      .withColumn("daysActive", datediff(col("endDate"),col("startDate")))
      .select("name", "type", "class", "navy", "country", "startDate", "endDate", "daysActive")
      .distinct()
  }
}
