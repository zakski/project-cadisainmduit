package com.szadowsz.cadisainmduit.places.grammar

/**
  * Created on 30/06/2017.
  */
case class DashSufffixGrTerm(name : String, patternWords : Seq[String], override val precedence : Int) extends GrTerm {

  def code : String = s"<<${name.toUpperCase}>>"

  override def conformPattern : String = patternWords.mkString("^(.*?)(-(?:","|","))$")

  override def remainderPattern: String = conformPattern

  override def conform(child : String, s : String): Option[String] = {
    val localPattern = conformRegex
    s match {
      case localPattern(g1,g2) => Option(s"$child$code")
      case _ => None
    }
  }

  override def remainder(s : String): Option[String] = {
    val localPattern = conformRegex
    s match {
      case localPattern(g1,g2) => Option(g1)
      case _ => None
    }
  }
}
