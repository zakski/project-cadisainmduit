package com.szadowsz.cadisainmduit.places.grammar

import scala.util.matching.Regex

/**
  * Created on 30/06/2017.
  */
case class PrefixSufffixGrTerm(name : String, patternWords : Seq[String], prefix : Boolean, override val precedence : Int) extends GrTerm {

  def code : String = s"<<${name.toUpperCase}>>"

  override def conformPattern : String = {
    if (prefix) patternWords.mkString("^((?:","|",") )(.*?)$") else patternWords.mkString("^(.*?)( (?:","|","))$")
  }

  override def remainderPattern: String = conformPattern

  override def conform(child : String, s : String): Option[String] = {
    val localPattern = conformRegex
    s match {
      case localPattern(g1,g2) => if (prefix) Option(s"$code $child") else Option(s"$child $code")
      case _ => None
    }
  }

  override def remainder(s : String): Option[String] = {
    val localPattern = conformRegex
    s match {
      case localPattern(g1,g2) => if (prefix) Option(g2) else Option(g1)
      case _ => None
    }
  }
}
