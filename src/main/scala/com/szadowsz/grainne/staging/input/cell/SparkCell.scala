package com.szadowsz.grainne.input.cell

import com.szadowsz.grainne.data.{County, Gender}
import com.szadowsz.grainne.input.util.spelling.BirthSpell
import com.szadowsz.grainne.staging.input.util.spelling.simple.{LangSpell, LitSpell, BeliefsSpell}
import com.szadowsz.grainne.tools.lang.WordFormatting
import org.supercsv.cellprocessor.CellProcessorAdaptor
import org.supercsv.util.CsvContext

object SparkCell {

  def apply(column: Header.Value): SparkCell = {
    column match {
      case Header.GENDER => new GenderCell
      case Header.AGE => new AgeCell
      case Header.CO => new CountyCell
      case Header.SUR => new NameCell
      case Header.FORE => new NameCell
      case Header.LANG => new LangCell
      case Header.REL => new ReligionCell
      case Header.OCC => new JobCell
      case Header.LIT => new LitCell
      case Header.BIRTH => new BirthCell
      case _ => new SparkCell
    }
  }
}

/**
  * General Apache Spark Csv Cell.
  *
  * @author Zakski : 15/02/2016.
  */
class SparkCell extends CellProcessorAdaptor with Serializable {

  /**
    * Method Wraps value in an Option to handle missing values.
    *
    * @param value   the column input value
    * @param context the data location context
    * @return an Optional Value
    */
  override def execute(value: Any, context: CsvContext): AnyRef = {
    Option(value) match {
      case Some(s: String) => Option(WordFormatting.capitalizeFully(s.trim))
      case _ => None
    }
  }
}

/**
  * Cell to process gender.
  *
  * @author Zakski : 29/07/2015.
  */
private class GenderCell extends SparkCell {

  override def execute(value: Any, context: CsvContext): AnyRef = {
    Option(value) match {
      case Some("M") => Option(Gender.MALE)
      case Some("F") => Option(Gender.FEMALE)
      case _ => None
    }
  }
}

/**
  * Cell to process age.
  *
  * @author Zakski : 11/11/2015.
  */
private class AgeCell extends SparkCell {

  override def execute(value: Any, context: CsvContext): AnyRef = {
    Option(value).map(_.toString.toInt)
  }
}

/**
  * Cell to process Irish Counties
  *
  * @author Zakski : 14/11/2015.
  */
private class CountyCell extends SparkCell {

  override def execute(value: Any, context: CsvContext): AnyRef = {
    Option(value) match {
      case Some("Londonderry") => Option(County.DERRY)
      case Some("Queen's Co.") => Option(County.LAOIS)
      case Some("King's Co.") => Option(County.OFFALY)
      case Some(county: String) => Option(County.fromString(county))
      case _ => None
    }
  }
}

/**
  * Cell to process names
  *
  * @author Zakski : 23/11/2015.
  */
private class NameCell extends SparkCell {

  private def split(field: String): List[String] = {
    val words = WordFormatting.formatName(field).split("[ -]").toList
    words.filter(w => w.length > 1 ||((w == "D" || w == "O") && words.head == w))
  }

  override def execute(value: Any, context: CsvContext): AnyRef = {
    Option(value) match {
      case Some(s: String) => Option(split(s.trim))
      case _ => None
    }
  }
}

/**
  * Cell to process Irish Knowledge.
  *
  * @author Zakski : 29/07/2015.
  */
private class LangCell extends SparkCell {

  override def execute(value: Any, context: CsvContext): AnyRef = {
    Option(value) match {
      case Some(s: String) =>
        val parsed = LangSpell.parse(s)
        parsed.split(" ").toList

      case _ => List("English") // We will assume that they only speak English if no value for knowsIrish is filled in.
    }
  }
}

/**
  * Cell to process religious affiliation.
  *
  * @author Zakski : 13/11/2015.
  */
private class ReligionCell extends SparkCell {

  override def execute(value: Any, context: CsvContext): AnyRef = {
    Option(value) match {
      case Some(s : String) => Option(BeliefsSpell.parse(s))
      case _ => None
    }
  }
}

/**
  * Cell to process religious affiliation.
  *
  * @author Zakski : 13/11/2015.
  */
private class LitCell extends SparkCell {

  override def execute(value: Any, context: CsvContext): AnyRef = {
    Option(value) match {
      case Some(s : String) => Option(LitSpell.parse(s))
      case _ => None
    }
  }
}

/**
  * Cell to process religious affiliation.
  *
  * @author Zakski : 13/11/2015.
  */
private class BirthCell extends SparkCell {

  override def execute(value: Any, context: CsvContext): AnyRef = {
    Option(value) match {
      case Some(s : String) => Option(BirthSpell.parse(WordFormatting.capitalizeFully(s.trim)))
      case _ => None
    }
  }
}


/**
  * Created by zakski on 13/11/2015.
  */
private class JobCell extends SparkCell {

  override def execute(value: Any, context: CsvContext): AnyRef = {
    Option(value) match {
      case Some(s : String) => Option(/*JobsSpelling.checkPhrase(*/WordFormatting.capitalizeFully(s.trim,List(' ','-',')','(','.')))//)
      case _ => None
    }
  }
}


