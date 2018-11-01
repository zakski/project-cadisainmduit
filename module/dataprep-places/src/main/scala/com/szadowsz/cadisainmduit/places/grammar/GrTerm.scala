package com.szadowsz.cadisainmduit.places.grammar

import scala.util.matching.Regex

/**
  * Created on 29/06/2017.
  */
trait GrTerm {

  def precedence : Int

  def code : String

  def conformPattern : String

  def remainderPattern : String

  def conformRegex: Regex = conformPattern.r

  def remainderRegex: Regex = remainderPattern.r

  def conform(child : String, s : String) : Option[String]

  def remainder(s : String): Option[String]
}
